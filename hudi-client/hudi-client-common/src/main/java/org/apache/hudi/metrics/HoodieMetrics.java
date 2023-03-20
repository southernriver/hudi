/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.metrics;

import org.apache.hudi.async.AsyncPostEventService;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import org.apache.hudi.tdbank.TdbankHoodieMetricsEvent;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Locale;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Wrapper for metrics-related operations.
 */
public class HoodieMetrics {

  private static final Logger LOG = LogManager.getLogger(HoodieMetrics.class);
  // Some timers
  public String rollbackTimerName = null;
  public String cleanTimerName = null;
  public String commitTimerName = null;
  public String deltaCommitTimerName = null;
  public String replaceCommitTimerName = null;
  public String finalizeTimerName = null;
  public String compactionTimerName = null;
  public String indexTimerName = null;
  private String conflictResolutionTimerName = null;
  private String conflictResolutionSuccessCounterName = null;
  private String conflictResolutionFailureCounterName = null;
  private HoodieWriteConfig config;
  private String tableName;
  // Add a job id to identify job for each hoodie table.
  private String hoodieJobId;
  private Timer rollbackTimer = null;
  private Timer cleanTimer = null;
  private Timer commitTimer = null;
  private Timer deltaCommitTimer = null;
  private Timer finalizeTimer = null;
  private Timer compactionTimer = null;
  private Timer clusteringTimer = null;
  private Timer indexTimer = null;
  private Timer conflictResolutionTimer = null;
  private Counter conflictResolutionSuccessCounter = null;
  private Counter conflictResolutionFailureCounter = null;

  // a queue for buffer metrics event.
  private final LinkedBlockingQueue<TdbankHoodieMetricsEvent> queue = new LinkedBlockingQueue<>();

  public HoodieMetrics(HoodieWriteConfig config) {
    this.config = config;
    this.tableName = config.getTableName();
    this.hoodieJobId = config.getHoodieJobId();
    if (config.isMetricsOn()) {
      // start post event service.
      AsyncPostEventService postEventService = new AsyncPostEventService(config, queue);
      postEventService.start(null);
      Metrics.init(config);
      this.rollbackTimerName = getMetricsName("timer", HoodieTimeline.ROLLBACK_ACTION);
      this.cleanTimerName = getMetricsName("timer", HoodieTimeline.CLEAN_ACTION);
      this.commitTimerName = getMetricsName("timer", HoodieTimeline.COMMIT_ACTION);
      this.deltaCommitTimerName = getMetricsName("timer", HoodieTimeline.DELTA_COMMIT_ACTION);
      this.replaceCommitTimerName = getMetricsName("timer", HoodieTimeline.REPLACE_COMMIT_ACTION);
      this.finalizeTimerName = getMetricsName("timer", "finalize");
      this.compactionTimerName = getMetricsName("timer", HoodieTimeline.COMPACTION_ACTION);
      this.indexTimerName = getMetricsName("timer", "index");
      this.conflictResolutionTimerName = getMetricsName("timer", "conflict_resolution");
      this.conflictResolutionSuccessCounterName = getMetricsName("counter", "conflict_resolution.success");
      this.conflictResolutionFailureCounterName = getMetricsName("counter", "conflict_resolution.failure");
    }
  }

  private Timer createTimer(String name) {
    return config.isMetricsOn() ? Metrics.getInstance().getRegistry().timer(name) : null;
  }

  public Timer.Context getRollbackCtx() {
    if (config.isMetricsOn() && rollbackTimer == null) {
      rollbackTimer = createTimer(rollbackTimerName);
    }
    return rollbackTimer == null ? null : rollbackTimer.time();
  }

  public Timer.Context getCompactionCtx() {
    if (config.isMetricsOn() && compactionTimer == null) {
      compactionTimer = createTimer(commitTimerName);
    }
    return compactionTimer == null ? null : compactionTimer.time();
  }

  public Timer.Context getClusteringCtx() {
    if (config.isMetricsOn() && clusteringTimer == null) {
      clusteringTimer = createTimer(replaceCommitTimerName);
    }
    return clusteringTimer == null ? null : clusteringTimer.time();
  }

  public Timer.Context getCleanCtx() {
    if (config.isMetricsOn() && cleanTimer == null) {
      cleanTimer = createTimer(cleanTimerName);
    }
    return cleanTimer == null ? null : cleanTimer.time();
  }

  public Timer.Context getCommitCtx() {
    if (config.isMetricsOn() && commitTimer == null) {
      commitTimer = createTimer(commitTimerName);
    }
    return commitTimer == null ? null : commitTimer.time();
  }

  public Timer.Context getFinalizeCtx() {
    if (config.isMetricsOn() && finalizeTimer == null) {
      finalizeTimer = createTimer(finalizeTimerName);
    }
    return finalizeTimer == null ? null : finalizeTimer.time();
  }

  public Timer.Context getDeltaCommitCtx() {
    if (config.isMetricsOn() && deltaCommitTimer == null) {
      deltaCommitTimer = createTimer(deltaCommitTimerName);
    }
    return deltaCommitTimer == null ? null : deltaCommitTimer.time();
  }

  public Timer.Context getIndexCtx() {
    if (config.isMetricsOn() && indexTimer == null) {
      indexTimer = createTimer(indexTimerName);
    }
    return indexTimer == null ? null : indexTimer.time();
  }

  public Timer.Context getConflictResolutionCtx() {
    if (config.isLockingMetricsEnabled() && conflictResolutionTimer == null) {
      conflictResolutionTimer = createTimer(conflictResolutionTimerName);
    }
    return conflictResolutionTimer == null ? null : conflictResolutionTimer.time();
  }

  public void updateMetricsForEmptyData(String actionType) {
    if (!config.isMetricsOn() || !config.getMetricsReporterType().equals(MetricsReporterType.PROMETHEUS_PUSHGATEWAY)) {
      // No-op if metrics are not of type PROMETHEUS_PUSHGATEWAY.
      return;
    }
    Metrics.registerGauge(getMetricsName(actionType, "totalPartitionsWritten"), 0);
    Metrics.registerGauge(getMetricsName(actionType, "totalFilesInsert"), 0);
    Metrics.registerGauge(getMetricsName(actionType, "totalFilesUpdate"), 0);
    Metrics.registerGauge(getMetricsName(actionType, "totalRecordsWritten"), 0);
    Metrics.registerGauge(getMetricsName(actionType, "totalUpdateRecordsWritten"), 0);
    Metrics.registerGauge(getMetricsName(actionType, "totalInsertRecordsWritten"), 0);
    Metrics.registerGauge(getMetricsName(actionType, "totalBytesWritten"), 0);
    Metrics.registerGauge(getMetricsName(actionType, "totalScanTime"), 0);
    Metrics.registerGauge(getMetricsName(actionType, "totalCreateTime"), 0);
    Metrics.registerGauge(getMetricsName(actionType, "totalUpsertTime"), 0);
    Metrics.registerGauge(getMetricsName(actionType, "totalCompactedRecordsUpdated"), 0);
    Metrics.registerGauge(getMetricsName(actionType, "totalLogFilesCompacted"), 0);
    Metrics.registerGauge(getMetricsName(actionType, "totalLogFilesSize"), 0);

    TdbankHoodieMetricsEvent metricEvent = TdbankHoodieMetricsEvent.newBuilder()
        .withDBName(config.getDatabaseName())
        .withTableName(config.getTableName())
        .withTableType(TdbankHoodieMetricsEvent.EventType.valueOf(actionType.toUpperCase(Locale.ROOT)))
        .addMetrics("totalPartitionsWritten", 0)
        .addMetrics("totalFilesUpdate", 0)
        .addMetrics("totalRecordsWritten", 0)
        .addMetrics("totalUpdateRecordsWritten", 0)
        .addMetrics("totalInsertRecordsWritten", 0)
        .addMetrics("totalBytesWritten", 0)
        .addMetrics("totalScanTime", 0)
        .addMetrics("totalCreateTime", 0)
        .addMetrics("totalUpsertTime", 0)
        .addMetrics("totalCompactedRecordsUpdated", 0)
        .addMetrics("totalLogFilesCompacted", 0)
        .addMetrics("totalLogFilesSize", 0)
        .build();
    postEvent(metricEvent);
  }

  public void updateCommitMetrics(long commitEpochTimeInMs, long durationInMs, HoodieCommitMetadata metadata,
      String actionType) {
    updateCommitTimingMetrics(commitEpochTimeInMs, durationInMs, metadata, actionType);
    if (config.isMetricsOn()) {
      long totalPartitionsWritten = metadata.fetchTotalPartitionsWritten();
      long totalFilesInsert = metadata.fetchTotalFilesInsert();
      long totalFilesUpdate = metadata.fetchTotalFilesUpdated();
      long totalRecordsWritten = metadata.fetchTotalRecordsWritten();
      long totalUpdateRecordsWritten = metadata.fetchTotalUpdateRecordsWritten();
      long totalInsertRecordsWritten = metadata.fetchTotalInsertRecordsWritten();
      long totalBytesWritten = metadata.fetchTotalBytesWritten();
      long totalTimeTakenByScanner = metadata.getTotalScanTime();
      long totalTimeTakenForInsert = metadata.getTotalCreateTime();
      long totalTimeTakenForUpsert = metadata.getTotalUpsertTime();
      long totalCompactedRecordsUpdated = metadata.getTotalCompactedRecordsUpdated();
      long totalLogFilesCompacted = metadata.getTotalLogFilesCompacted();
      long totalLogFilesSize = metadata.getTotalLogFilesSize();
      Metrics.registerGauge(getMetricsName(actionType, "totalPartitionsWritten"), totalPartitionsWritten);
      Metrics.registerGauge(getMetricsName(actionType, "totalFilesInsert"), totalFilesInsert);
      Metrics.registerGauge(getMetricsName(actionType, "totalFilesUpdate"), totalFilesUpdate);
      Metrics.registerGauge(getMetricsName(actionType, "totalRecordsWritten"), totalRecordsWritten);
      Metrics.registerGauge(getMetricsName(actionType, "totalUpdateRecordsWritten"), totalUpdateRecordsWritten);
      Metrics.registerGauge(getMetricsName(actionType, "totalInsertRecordsWritten"), totalInsertRecordsWritten);
      Metrics.registerGauge(getMetricsName(actionType, "totalBytesWritten"), totalBytesWritten);
      Metrics.registerGauge(getMetricsName(actionType, "totalScanTime"), totalTimeTakenByScanner);
      Metrics.registerGauge(getMetricsName(actionType, "totalCreateTime"), totalTimeTakenForInsert);
      Metrics.registerGauge(getMetricsName(actionType, "totalUpsertTime"), totalTimeTakenForUpsert);
      Metrics.registerGauge(getMetricsName(actionType, "totalCompactedRecordsUpdated"), totalCompactedRecordsUpdated);
      Metrics.registerGauge(getMetricsName(actionType, "totalLogFilesCompacted"), totalLogFilesCompacted);
      Metrics.registerGauge(getMetricsName(actionType, "totalLogFilesSize"), totalLogFilesSize);

      TdbankHoodieMetricsEvent metricEvent = TdbankHoodieMetricsEvent.newBuilder()
          .withDBName(config.getDatabaseName())
          .withTableName(config.getTableName())
          .withTableType(TdbankHoodieMetricsEvent.EventType.valueOf(actionType.toUpperCase(Locale.ROOT)))
          .addMetrics("totalPartitionsWritten", totalPartitionsWritten)
          .addMetrics("totalFilesUpdate", totalFilesUpdate)
          .addMetrics("totalFilesInsert", totalFilesInsert)
          .addMetrics("totalRecordsWritten", totalRecordsWritten)
          .addMetrics("totalUpdateRecordsWritten", totalUpdateRecordsWritten)
          .addMetrics("totalInsertRecordsWritten", totalInsertRecordsWritten)
          .addMetrics("totalBytesWritten", totalBytesWritten)
          .addMetrics("totalScanTime", totalTimeTakenByScanner)
          .addMetrics("totalCreateTime", totalTimeTakenForInsert)
          .addMetrics("totalUpsertTime", totalTimeTakenForUpsert)
          .addMetrics("totalCompactedRecordsUpdated", totalCompactedRecordsUpdated)
          .addMetrics("totalLogFilesCompacted", totalLogFilesCompacted)
          .addMetrics("totalLogFilesSize", totalLogFilesSize)
          .build();
      postEvent(metricEvent);
    }
  }

  private void updateCommitTimingMetrics(long commitEpochTimeInMs, long durationInMs, HoodieCommitMetadata metadata,
      String actionType) {
    if (config.isMetricsOn()) {
      TdbankHoodieMetricsEvent.Builder builder = TdbankHoodieMetricsEvent.newBuilder()
          .withDBName(config.getDatabaseName())
          .withTableName(config.getTableName())
          .withTableType(TdbankHoodieMetricsEvent.EventType.valueOf(actionType.toUpperCase(Locale.ROOT)));
      Pair<Option<Long>, Option<Long>> eventTimePairMinMax = metadata.getMinAndMaxEventTime();
      if (eventTimePairMinMax.getLeft().isPresent()) {
        long commitLatencyInMs = commitEpochTimeInMs + durationInMs - eventTimePairMinMax.getLeft().get();
        Metrics.registerGauge(getMetricsName(actionType, "commitLatencyInMs"), commitLatencyInMs);
        builder = builder.addMetrics("commitLatencyInMs", commitLatencyInMs);
      }
      if (eventTimePairMinMax.getRight().isPresent()) {
        long commitFreshnessInMs = commitEpochTimeInMs + durationInMs - eventTimePairMinMax.getRight().get();
        Metrics.registerGauge(getMetricsName(actionType, "commitFreshnessInMs"), commitFreshnessInMs);
        builder = builder.addMetrics("commitFreshnessInMs", commitFreshnessInMs);
      }
      Metrics.registerGauge(getMetricsName(actionType, "commitTime"), commitEpochTimeInMs);
      Metrics.registerGauge(getMetricsName(actionType, "duration"), durationInMs);

      TdbankHoodieMetricsEvent event = builder
          .addMetrics("commitTime", commitEpochTimeInMs)
          .addMetrics("duration", durationInMs)
          .build();
      postEvent(event);
    }
  }

  public void updateRollbackMetrics(long durationInMs, long numFilesDeleted) {
    if (config.isMetricsOn()) {
      LOG.info(
          String.format("Sending rollback metrics (duration=%d, numFilesDeleted=%d)", durationInMs, numFilesDeleted));
      Metrics.registerGauge(getMetricsName("rollback", "duration"), durationInMs);
      Metrics.registerGauge(getMetricsName("rollback", "numFilesDeleted"), numFilesDeleted);
      TdbankHoodieMetricsEvent event = TdbankHoodieMetricsEvent.newBuilder()
          .withDBName(config.getDatabaseName())
          .withTableName(config.getTableName())
          .withTableType(TdbankHoodieMetricsEvent.EventType.valueOf("rollback".toUpperCase(Locale.ROOT)))
          .addMetrics("duration", durationInMs)
          .addMetrics("numFilesDeleted", numFilesDeleted)
          .build();
      postEvent(event);
    }
  }

  public void updateCleanMetrics(long durationInMs, int numFilesDeleted) {
    if (config.isMetricsOn()) {
      LOG.info(
          String.format("Sending clean metrics (duration=%d, numFilesDeleted=%d)", durationInMs, numFilesDeleted));
      Metrics.registerGauge(getMetricsName("clean", "duration"), durationInMs);
      Metrics.registerGauge(getMetricsName("clean", "numFilesDeleted"), numFilesDeleted);
      TdbankHoodieMetricsEvent event = TdbankHoodieMetricsEvent.newBuilder()
          .withDBName(config.getDatabaseName())
          .withTableName(config.getTableName())
          .withTableType(TdbankHoodieMetricsEvent.EventType.valueOf("clean".toUpperCase(Locale.ROOT)))
          .addMetrics("duration", durationInMs)
          .addMetrics("numFilesDeleted", numFilesDeleted)
          .build();
      postEvent(event);
    }
  }

  public void updateFinalizeWriteMetrics(long durationInMs, long numFilesFinalized) {
    if (config.isMetricsOn()) {
      LOG.info(String.format("Sending finalize write metrics (duration=%d, numFilesFinalized=%d)", durationInMs,
          numFilesFinalized));
      Metrics.registerGauge(getMetricsName("finalize", "duration"), durationInMs);
      Metrics.registerGauge(getMetricsName("finalize", "numFilesFinalized"), numFilesFinalized);
      TdbankHoodieMetricsEvent event = TdbankHoodieMetricsEvent.newBuilder()
          .withDBName(config.getDatabaseName())
          .withTableName(config.getTableName())
          .withTableType(TdbankHoodieMetricsEvent.EventType.valueOf("finalize".toUpperCase(Locale.ROOT)))
          .addMetrics("duration", durationInMs)
          .addMetrics("numFilesFinalized", numFilesFinalized)
          .build();
      postEvent(event);
    }
  }

  public void updateIndexMetrics(final String action, final long durationInMs) {
    if (config.isMetricsOn()) {
      LOG.info(String.format("Sending index metrics (%s.duration, %d)", action, durationInMs));
      Metrics.registerGauge(getMetricsName("index", String.format("%s.duration", action)), durationInMs);
      TdbankHoodieMetricsEvent event = TdbankHoodieMetricsEvent.newBuilder()
          .withDBName(config.getDatabaseName())
          .withTableName(config.getTableName())
          .withTableType(TdbankHoodieMetricsEvent.EventType.valueOf("index".toUpperCase(Locale.ROOT)))
          .addMetrics(String.format("%s.duration", action), durationInMs)
          .build();
      postEvent(event);
    }
  }

  String getMetricsName(String action, String metric) {
    // if using zhiyan, then we don't report metrics prefix because we will use tags to identify each metrics
    return config == null ? null :
      config.getMetricsReporterType() == MetricsReporterType.ZHIYAN ? String.format("%s.%s", action, metric) :
        String.format("%s.%s.%s", config.getMetricReporterMetricsNamePrefix(), action, metric);
  }

  /**
   * By default, the timer context returns duration with nano seconds. Convert it to millisecond.
   */
  public long getDurationInMs(long ctxDuration) {
    return ctxDuration / 1000000;
  }

  public void emitConflictResolutionSuccessful() {
    if (config.isLockingMetricsEnabled()) {
      LOG.info("Sending conflict resolution success metric");
      conflictResolutionSuccessCounter = getCounter(conflictResolutionSuccessCounter, conflictResolutionSuccessCounterName);
      conflictResolutionSuccessCounter.inc();
    }
  }

  public void emitConflictResolutionFailed() {
    if (config.isLockingMetricsEnabled()) {
      LOG.info("Sending conflict resolution failure metric");
      conflictResolutionFailureCounter = getCounter(conflictResolutionFailureCounter, conflictResolutionFailureCounterName);
      conflictResolutionFailureCounter.inc();
    }
  }

  private Counter getCounter(Counter counter, String name) {
    if (counter == null) {
      return Metrics.getInstance().getRegistry().counter(name);
    }
    return counter;
  }

  private void postEvent(TdbankHoodieMetricsEvent event) {
    LOG.info("Post metrics event to queue, queue size now is " + queue.size());
    queue.add(event);
  }
}
