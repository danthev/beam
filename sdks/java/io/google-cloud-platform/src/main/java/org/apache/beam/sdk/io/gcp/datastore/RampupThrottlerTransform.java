package org.apache.beam.sdk.io.gcp.datastore;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.MovingFunction;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of a client-side throttler that enforces a gradual ramp-up, broadly in line
 * with Datastore best practices. See also https://cloud.google.com/datastore/docs/best-practices#ramping_up_traffic.
 */
public class RampupThrottlerTransform<T> extends
    PTransform<PCollection<T>, PCollection<T>> implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(RampupThrottlerTransform.class);
  private static final double BASE_BUDGET = 500.0;
  private static final Duration RAMP_UP_INTERVAL = Duration.standardMinutes(5);
  private static final FluentBackoff fluentBackoff = FluentBackoff.DEFAULT;

  private final int numShards;

  public RampupThrottlerTransform(int numShards) {
    this.numShards = numShards;
  }

  @Override
  public PCollection<T> expand(PCollection<T> input) {
    return input
        .apply(ParDo.of(new BatchElementsFn(100 * 1000)))
        .apply("Throttle for ramp-up", ParDo.of(new SplittableRampupThrottlingFn(numShards)));
  }

  class SplittableRampupThrottlingFn extends DoFn<List<T>, T> {

    private int numShards;

    // Initialized on Beam setup.
    private transient MovingFunction successfulOps;
    private Instant firstOpInstant;

    private final Counter throttlingMsecs =
        Metrics.counter(RampupThrottlingFn.class, "throttling-msecs");

    private SplittableRampupThrottlingFn(int numShards) {
      this.numShards = numShards;
      this.successfulOps = new MovingFunction(
          Duration.standardSeconds(1).getMillis(),
          Duration.standardSeconds(1).getMillis(),
          1 /* numSignificantBuckets */,
          1 /* numSignificantSamples */,
          Sum.ofLongs());
      this.firstOpInstant = Instant.now();
    }

    // 500 / numShards * 1.5^max(0, (x-5)/5)
    private int calcMaxOpsBudget(Instant first, Instant instant) {
      double rampUpIntervalMinutes = (double) RAMP_UP_INTERVAL.getStandardMinutes();
      Duration durationSinceFirst = new Duration(first, instant);

      double calculatedGrowth = (durationSinceFirst.getStandardMinutes() - rampUpIntervalMinutes)
          / rampUpIntervalMinutes;
      double growth = Math.max(0, calculatedGrowth);
      double maxOpsBudget = BASE_BUDGET / this.numShards * Math.pow(1.5, growth);
      return (int) maxOpsBudget;
    }

    @Setup
    public void setup() {
      this.successfulOps = new MovingFunction(
          Duration.standardSeconds(1).getMillis(),
          Duration.standardSeconds(1).getMillis(),
          1 /* numSignificantBuckets */,
          1 /* numSignificantSamples */,
          Sum.ofLongs());
      this.firstOpInstant = Instant.now();
    }

    @StartBundle
    public void startBundle(StartBundleContext c) {
      LOG.info("{} options", c.getPipelineOptions().outputRuntimeOptions().size());
      c.getPipelineOptions().outputRuntimeOptions().forEach((k, v) -> LOG.info("{}: {}", k, v));
    }

    @ProcessElement
    public void process(ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker)
        throws IOException, InterruptedException {
      Instant nonNullableFirstInstant = firstOpInstant;

      Sleeper sleeper = Sleeper.DEFAULT;
      BackOff backoff = fluentBackoff.backoff();
      long i = tracker.currentRestriction().getFrom();
      while(i < tracker.currentRestriction().getTo()){
        Instant instant = Instant.now();
        int maxOpsBudget = calcMaxOpsBudget(nonNullableFirstInstant, instant);
        LOG.info("{} shards, local max budget is {} entities/s after {}s", numShards, maxOpsBudget,
            new Duration(nonNullableFirstInstant, instant).getStandardSeconds());
        long currentOpCount = successfulOps.get(instant.getMillis());
        long availableOps = maxOpsBudget - currentOpCount;

        if(availableOps > 0) {
          int emittedOps = 0;
          while (availableOps > 0) {
            if(!tracker.tryClaim(i))
              return;
            T element = c.element().get((int) i);
            i++;

            c.output(element);
            backoff.reset();
            emittedOps++;
            availableOps--;
          }
          successfulOps.add(instant.getMillis(), emittedOps);
        } else {
          long backoffMillis = backoff.nextBackOffMillis();
          LOG.info("Delaying by {}ms to conform to gradual ramp-up.", backoffMillis);
          throttlingMsecs.inc(backoffMillis);
          sleeper.sleep(backoffMillis);
        }
      }
    }

    @NewTracker
    public OffsetRangeTracker newTracker(@Restriction OffsetRange range) {
      return new OffsetRangeTracker(range);
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(@Element List<T> element) {
      return new OffsetRange(0L, element.size());
    }

    @SplitRestriction
    public void splitRestriction(@Element List<T> element, @Restriction OffsetRange restriction,
        OutputReceiver<OffsetRange> receiver) {
      long chunkSize = (long) Math.ceil(1.0 * (restriction.getTo() - restriction.getFrom()) / numShards);
      LOG.info("Splitting {} into chunks of {}", restriction.getTo() - restriction.getFrom(), chunkSize);
      for (long i = restriction.getFrom(); i < restriction.getTo(); i+=chunkSize) {
        receiver.output(new OffsetRange(i, Math.min(restriction.getTo(), i+chunkSize)));
      }
    }

  }

  class BatchElementsFn extends DoFn<T, List<T>> {

      private final List<T> elements;
      private final int batchSize;

      private BatchElementsFn(int batchSize){
        this.batchSize = batchSize;
        elements = new ArrayList<>(batchSize);
      }

      @StartBundle
      public void startBundle(){
        // elements.clear();
      }

      @ProcessElement
      public void processElement(ProcessContext c){
        elements.add(c.element());
        if (elements.size() >= batchSize){
          c.output(elements);
          elements.clear();
        }
      }

      @FinishBundle
      public void finishBundle(FinishBundleContext c){
        // if (elements.size() > 0){
        //   c.output(elements, BoundedWindow.TIMESTAMP_MIN_VALUE, GlobalWindow.INSTANCE);
        //   elements.clear();
        // }
      }

  }

  class RampupThrottlingFn extends DoFn<KV<Integer, Iterable<T>>, T> implements Serializable {

    private final int numShards;

    // Initialized on Beam setup.
    private transient MovingFunction successfulOps;
    private Instant firstOpInstant;

    private final Counter throttlingMsecs =
        Metrics.counter(RampupThrottlingFn.class, "throttling-msecs");

    private RampupThrottlingFn(int numShards) {
      this.numShards = numShards;
      this.successfulOps = new MovingFunction(
          Duration.standardSeconds(1).getMillis(),
          Duration.standardSeconds(1).getMillis(),
          1 /* numSignificantBuckets */,
          1 /* numSignificantSamples */,
          Sum.ofLongs());
      this.firstOpInstant = Instant.now();
    }

    // 500 / numShards * 1.5^max(0, (x-5)/5)
    private int calcMaxOpsBudget(Instant first, Instant instant) {
      double rampUpIntervalMinutes = (double) RAMP_UP_INTERVAL.getStandardMinutes();
      Duration durationSinceFirst = new Duration(first, instant);

      double calculatedGrowth = (durationSinceFirst.getStandardMinutes() - rampUpIntervalMinutes)
          / rampUpIntervalMinutes;
      double growth = Math.max(0, calculatedGrowth);
      double maxOpsBudget = BASE_BUDGET / this.numShards * Math.pow(1.5, growth);
      return (int) maxOpsBudget;
    }

    @Setup
    public void setup() {
      this.successfulOps = new MovingFunction(
          Duration.standardSeconds(1).getMillis(),
          Duration.standardSeconds(1).getMillis(),
          1 /* numSignificantBuckets */,
          1 /* numSignificantSamples */,
          Sum.ofLongs());
      this.firstOpInstant = Instant.now();
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException, InterruptedException {
      Instant nonNullableFirstInstant = firstOpInstant;

      int shard_id = c.element().getKey();
      Iterator<T> elementsIter = c.element().getValue().iterator();
      Sleeper sleeper = Sleeper.DEFAULT;
      BackOff backoff = fluentBackoff.backoff();
      while (elementsIter.hasNext()) {
        Instant instant = Instant.now();
        int maxOpsBudget = calcMaxOpsBudget(nonNullableFirstInstant, instant);
        // LOG.debug("Shard {}: Max budget is {} entities/s after {}s", shard_id, maxOpsBudget,
        //     new Duration(nonNullableFirstInstant, instant).getStandardSeconds());
        long currentOpCount = successfulOps.get(instant.getMillis());
        long availableOps = maxOpsBudget - currentOpCount;

        if(availableOps > 0) {
          int emittedOps = 0;
          while (availableOps > 0 && elementsIter.hasNext()) {
            T element = elementsIter.next();

            c.output(element);
            backoff.reset();
            emittedOps++;
            availableOps--;
          }
          successfulOps.add(instant.getMillis(), emittedOps);
        } else {
          long backoffMillis = backoff.nextBackOffMillis();
          LOG.info("Delaying by {}ms to conform to gradual ramp-up.", backoffMillis);
          throttlingMsecs.inc(backoffMillis);
          sleeper.sleep(backoffMillis);
        }
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.add(
          DisplayData.item("numThrottlerShards", numShards)
              .withLabel("Number of shards for ramp-up throttling"));
    }

  }

}
