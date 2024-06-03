/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.export;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.DataFilePartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.rds.model.LoadStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class DataFileScheduler implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(DataFileScheduler.class);

    private final AtomicInteger numOfWorkers = new AtomicInteger(0);

    /**
     * Maximum concurrent data loader per node
     */
    private static final int MAX_JOB_COUNT = 1;

    /**
     * Default interval to acquire a lease from coordination store
     */
    private static final int DEFAULT_LEASE_INTERVAL_MILLIS = 2_000;


    private final EnhancedSourceCoordinator sourceCoordinator;
    private final ExecutorService executor;
    private final RdsSourceConfig sourceConfig;
    private final S3Client s3Client;
    private final EventFactory eventFactory;
    private final Buffer<Record<Event>> buffer;


    public DataFileScheduler(final EnhancedSourceCoordinator sourceCoordinator,
                             final RdsSourceConfig sourceConfig,
                             final S3Client s3Client,
                             final EventFactory eventFactory,
                             final Buffer<Record<Event>> buffer) {
        this.sourceCoordinator = sourceCoordinator;
        this.sourceConfig = sourceConfig;
        this.s3Client = s3Client;
        this.eventFactory = eventFactory;
        this.buffer = buffer;
        executor = Executors.newFixedThreadPool(MAX_JOB_COUNT);
    }

    @Override
    public void run() {
        LOG.debug("Starting Data File Scheduler to process S3 data files for export");

        while (!Thread.currentThread().isInterrupted()) {
            try {
                if (numOfWorkers.get() < MAX_JOB_COUNT) {
                    final Optional<EnhancedSourcePartition> sourcePartition = sourceCoordinator.acquireAvailablePartition(DataFilePartition.PARTITION_TYPE);

                    if (sourcePartition.isPresent()) {
                        LOG.debug("Acquired data file partition");
                        DataFilePartition dataFilePartition = (DataFilePartition) sourcePartition.get();
                        LOG.debug("Start processing data file partition");
                        processDataFilePartition(dataFilePartition);
                    }
                }
                try {
                    Thread.sleep(DEFAULT_LEASE_INTERVAL_MILLIS);
                } catch (final InterruptedException e) {
                    LOG.info("The DataFileScheduler was interrupted while waiting to retry, stopping processing");
                    break;
                }
            } catch (final Exception e) {
                LOG.error("Received an exception while processing an S3 data file, backing off and retrying", e);
                try {
                    Thread.sleep(DEFAULT_LEASE_INTERVAL_MILLIS);
                } catch (final InterruptedException ex) {
                    LOG.info("The DataFileScheduler was interrupted while waiting to retry, stopping processing");
                    break;
                }
            }
        }
        LOG.warn("Data file scheduler is interrupted, stopping all data file loaders...");
        // Cannot call executor.shutdownNow() here
        // Otherwise the final checkpoint will fail due to SDK interruption.
        executor.shutdown();
//        DataFileLoader.stopAll();
    }

    private void processDataFilePartition(DataFilePartition dataFilePartition) {
        Runnable loader = new DataFileLoader(dataFilePartition, s3Client, eventFactory, buffer);
        CompletableFuture runLoader = CompletableFuture.runAsync(loader, executor);

        runLoader.whenComplete((v, ex) -> {
            if (ex == null) {
                // TODO: update global state
                updateLoadStatus(dataFilePartition.getExportTaskId());
                sourceCoordinator.completePartition(dataFilePartition);
            } else {
                LOG.error("There was an exception while processing an S3 data file: {}", ex);
                sourceCoordinator.giveUpPartition(dataFilePartition);
            }
            numOfWorkers.decrementAndGet();
        });
        numOfWorkers.incrementAndGet();
    }

    private void updateLoadStatus(String exportTaskId) {
        while (true) {
            Optional<EnhancedSourcePartition> globalStatePartition = sourceCoordinator.getPartition(exportTaskId);
            if (globalStatePartition.isEmpty()) {
                LOG.error("Failed to get data file load status for {}", exportTaskId);
                return;
            }

            GlobalState globalState = (GlobalState) globalStatePartition.get();
            LoadStatus loadStatus = LoadStatus.fromMap(globalState.getProgressState().get());
            loadStatus.setLoadedFiles(loadStatus.getLoadedFiles() + 1);
            LOG.info("Current status: total {} loaded {}", loadStatus.getTotalFiles(), loadStatus.getLoadedFiles());

            globalState.setProgressState(loadStatus.toMap());

            try {
                sourceCoordinator.saveProgressStateForPartition(globalState, null);
                // if all load are completed.
                if (sourceConfig.isStreamEnabled() && loadStatus.getLoadedFiles() == loadStatus.getTotalFiles()) {
                    LOG.info("All Exports are done, streaming can continue...");
                    sourceCoordinator.createPartition(new GlobalState("stream-for-" + sourceConfig.getDbIdentifier(), null));
                }
                break;
            } catch (Exception e) {
                LOG.error("Failed to update the global status, looks like the status was out of date, will retry..");
            }
        }
    }
}
