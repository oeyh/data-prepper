/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.export;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.DataFilePartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.ExportPartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.DataFileProgressState;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.ExportProgressState;
import org.opensearch.dataprepper.plugins.source.rds.model.SnapshotInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class ExportScheduler implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ExportScheduler.class);

    private static final int DEFAULT_TAKE_LEASE_INTERVAL_MILLIS = 60_000;
    private static final Duration DEFAULT_CLOSE_DURATION = Duration.ofMinutes(10);
    private static final int DEFAULT_MAX_CLOSE_COUNT = 36;
    private static final int DEFAULT_CHECKPOINT_INTERVAL_MILLS = 5 * 60_000;
    private static final int DEFAULT_CHECK_STATUS_INTERVAL_MILLS = 30 * 1000;
    private static final String PARQUET_SUFFIX = ".parquet";

    private final RdsClient rdsClient;
    private final S3Client s3Client;

    private final PluginMetrics pluginMetrics;

    private final EnhancedSourceCoordinator sourceCoordinator;

    private final ExecutorService executor;

    private final ExportTaskManager exportTaskManager;
    private final SnapshotManager snapshotManager;

    public ExportScheduler(final EnhancedSourceCoordinator sourceCoordinator,
                           final RdsClient rdsClient,
                           final S3Client s3Client,
                           final PluginMetrics pluginMetrics) {
        this.pluginMetrics = pluginMetrics;
        this.sourceCoordinator = sourceCoordinator;
        this.rdsClient = rdsClient;
        this.s3Client = s3Client;
        this.executor = Executors.newCachedThreadPool();
        this.exportTaskManager = new ExportTaskManager(rdsClient);
        this.snapshotManager = new SnapshotManager(rdsClient);
    }

    @Override
    public void run() {
        LOG.debug("Start running Export Scheduler");
        while (!Thread.currentThread().isInterrupted()) {
            try {
                final Optional<EnhancedSourcePartition> sourcePartition = sourceCoordinator.acquireAvailablePartition(ExportPartition.PARTITION_TYPE);
                
                if (sourcePartition.isPresent()) {
                    ExportPartition exportPartition = (ExportPartition) sourcePartition.get();
                    LOG.debug("Acquired an export partition: " + exportPartition.getPartitionKey());

                    String exportTaskId = getOrCreateExportTaskId(exportPartition);

                    if (exportTaskId == null) {
                        LOG.error("The export to S3 failed, it will be retried");
                        closeExportPartitionWithError(exportPartition);
                    } else {
                        CompletableFuture<String> checkStatus = CompletableFuture.supplyAsync(() -> checkExportStatus(exportPartition), executor);
                        checkStatus.whenComplete(completeExport(exportPartition));
                    }
                }

                try {
                    Thread.sleep(DEFAULT_TAKE_LEASE_INTERVAL_MILLIS);
                } catch (final InterruptedException e) {
                    LOG.info("The ExportScheduler was interrupted while waiting to retry, stopping processing");
                    break;
                }
            } catch (final Exception e) {
                LOG.error("Received an exception during export, backing off and retrying", e);
                try {
                    Thread.sleep(DEFAULT_TAKE_LEASE_INTERVAL_MILLIS);
                } catch (final InterruptedException ex) {
                    LOG.info("The ExportScheduler was interrupted while waiting to retry, stopping processing");
                    break;
                }
            }

        }
        LOG.warn("Export scheduler interrupted, looks like shutdown has triggered");
        executor.shutdownNow();
    }

    private BiConsumer<String, Throwable> completeExport(ExportPartition exportPartition) {
        return (status, ex) -> {
            if (ex != null) {
                LOG.warn("Check export status for {} failed", exportPartition.getPartitionKey(), ex);
                sourceCoordinator.giveUpPartition(exportPartition);
            } else {
                if (!"COMPLETE".equals(status)) {
                    LOG.error("Export failed with status {}", status);
                    closeExportPartitionWithError(exportPartition);
                    return;
                }
                LOG.info("Export for {} completed successfully", exportPartition.getPartitionKey());

                ExportProgressState state = exportPartition.getProgressState().get();
                String bucket = state.getBucket();
                String prefix = state.getPrefix();
                String exportTaskId = state.getExportTaskId();

                List<String> dataFileObjectKeys = getDataFileObjectKeys(bucket, prefix, exportTaskId);
                createDataFilePartitions(bucket, exportTaskId, dataFileObjectKeys);

                completeExportPartition(exportPartition);
            }
        };
    }

    private List<String> getDataFileObjectKeys(String bucket, String prefix, String exportTaskId) {
        LOG.debug("Fetching object keys for export data files.");
        ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(prefix + "/" + exportTaskId);

        List<String> objectKeys = new ArrayList<>();
        ListObjectsV2Response response = null;
        do {
            String nextToken = response == null ? null : response.nextContinuationToken();
            response = s3Client.listObjectsV2(requestBuilder
                    .continuationToken(nextToken)
                    .build());
            objectKeys.addAll(response.contents().stream()
                    .map(S3Object::key)
                    .filter(key -> key.endsWith(PARQUET_SUFFIX))
                    .collect(Collectors.toList()));

        } while (response.isTruncated());
        return objectKeys;
    }

    private void createDataFilePartitions(String bucket, String exportTaskId, List<String> dataFileObjectKeys) {
        LOG.info("Total of {} data files generated for export {}", dataFileObjectKeys.size(), exportTaskId);
        AtomicInteger totalFiles = new AtomicInteger();
        for (final String objectKey : dataFileObjectKeys) {
            DataFileProgressState progressState = new DataFileProgressState();
            progressState.setTotal(1);
            progressState.setLoaded(0);
            // objectKey has this structure: "{prefix}/{export task ID}/{database name}/{table name}/...", table name is the 4th part
            String table = objectKey.split("/")[3];
            progressState.setSourceTable(table);

            DataFilePartition dataFilePartition = new DataFilePartition(exportTaskId, bucket, objectKey, Optional.of(progressState));
            sourceCoordinator.createPartition(dataFilePartition);
        }
    }

    private void completeExportPartition(ExportPartition exportPartition) {
        ExportProgressState progressState = exportPartition.getProgressState().get();
        progressState.setStatus("Completed");
        sourceCoordinator.completePartition(exportPartition);
    }

    private String getOrCreateExportTaskId(ExportPartition exportPartition) {
        ExportProgressState progressState = exportPartition.getProgressState().get();

        if (progressState.getExportTaskId() != null) {
            LOG.info("Export task has already created for db {} with export time {}", exportPartition.getDbIdentifier(), exportPartition.getExportTime());
            return progressState.getExportTaskId();
        }

        LOG.info("Creating a new snapshot for db {}", exportPartition.getDbIdentifier());
        SnapshotInfo snapshotInfo = snapshotManager.createSnapshot(exportPartition.getDbIdentifier());
        if (snapshotInfo == null) {
            LOG.error("The snapshot failed to create, it will be retried");
            closeExportPartitionWithError(exportPartition);
            return null;
        }

        String snapshotId = snapshotInfo.getSnapshotId();
        try {
            checkSnapshotStatus(snapshotId);
        } catch (Exception e) {
            LOG.warn("Check snapshot status for {} failed", snapshotId, e);
            sourceCoordinator.giveUpPartition(exportPartition);
        }

        LOG.info("Creating a new export task for db {} with export time {}", exportPartition.getDbIdentifier(), snapshotInfo.getCreateTime());
        // submit a new export request
        String exportTaskId = exportTaskManager.startExportTask(
                snapshotInfo.getSnapshotArn(), progressState.getIamRoleArn(), progressState.getBucket(),
                progressState.getPrefix(), progressState.getKmsKeyId(), progressState.getIncludeTables());

        if (exportTaskId != null) {
            LOG.info("Export task Id is " + exportTaskId);
            progressState.setExportTaskId(exportTaskId);
            sourceCoordinator.saveProgressStateForPartition(exportPartition, null);
        }
        return exportTaskId;
    }

    private String checkSnapshotStatus(String snapshotId) {
        LOG.debug("Start checking status of snapshot {}", snapshotId);
        while (true) {
            SnapshotInfo snapshotInfo = snapshotManager.checkSnapshotStatus(snapshotId);
            String status = snapshotInfo.getStatus();
            // Valid snapshot statuses are: available, copying, creating
            // The status should never be "copying" here
            if ("available".equals(status)) {
                LOG.info("Snapshot {} is available.", snapshotId);
                return status;
            }

            LOG.debug("Snapshot {} is still creating. Wait and check later", snapshotId);
            try {
                Thread.sleep(DEFAULT_CHECK_STATUS_INTERVAL_MILLS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private BiConsumer<String, Throwable> completeSnapshot(ExportPartition exportPartition, String snapshotId) {
        return (status, ex) -> {
            if (ex != null) {
                LOG.warn("Check snapshot status for {} failed", snapshotId, ex);
                sourceCoordinator.giveUpPartition(exportPartition);
            } else {
                // Snapshot created successfully
            }
        };
    }

    private void closeExportPartitionWithError(ExportPartition exportPartition) {
        ExportProgressState exportProgressState = exportPartition.getProgressState().get();
        // Clear current task id, so that a new export can be submitted.
        exportProgressState.setExportTaskId(null);
        sourceCoordinator.closePartition(exportPartition, DEFAULT_CLOSE_DURATION, DEFAULT_MAX_CLOSE_COUNT);
    }

    private String checkExportStatus(ExportPartition exportPartition) {
        long lastCheckpointTime = System.currentTimeMillis();
        String exportTaskId = exportPartition.getProgressState().get().getExportTaskId();

        LOG.debug("Start checking the status of export " + exportTaskId);
        while (true) {
            if (System.currentTimeMillis() - lastCheckpointTime > DEFAULT_CHECKPOINT_INTERVAL_MILLS) {
                sourceCoordinator.saveProgressStateForPartition(exportPartition, null);
                lastCheckpointTime = System.currentTimeMillis();
            }

            // Valid statuses are: CANCELED, CANCELING, COMPLETE, FAILED, IN_PROGRESS, STARTING
            String status = exportTaskManager.checkExportStatus(exportTaskId);
            LOG.debug("Current export status is {}.", status);
            if ("COMPLETE".equals(status) || "FAILED".equals(status) || "CANCELED".equals(status)) {
                LOG.info("Export {} is completed with final status {}", exportTaskId, status);
                return status;
            }
            LOG.debug("Export {} is still running in progress. Wait and check later", exportTaskId);
            try {
                Thread.sleep(DEFAULT_CHECK_STATUS_INTERVAL_MILLS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

    }
}
