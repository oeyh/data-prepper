/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.resync;

import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.converter.RecordConverter;
import org.opensearch.dataprepper.plugins.source.rds.converter.StreamRecordConverter;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.ResyncPartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.ResyncProgressState;
import org.opensearch.dataprepper.plugins.source.rds.schema.QueryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ResyncScheduler implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ResyncScheduler.class);

    private static final int DEFAULT_TAKE_LEASE_INTERVAL_MILLIS = 60_000;
    static final String DATA_PREPPER_EVENT_TYPE = "event";
    static final String EVENT_TYPE = "resync";
    static final Duration BUFFER_TIMEOUT = Duration.ofSeconds(60);
    static final int DEFAULT_BUFFER_BATCH_SIZE = 1_000;

    private final EnhancedSourceCoordinator sourceCoordinator;
    private final RdsSourceConfig sourceConfig;
    private final QueryManager queryManager;
    private final String s3Prefix;
    private final RecordConverter recordConverter;
    private final BufferAccumulator<Record<Event>> bufferAccumulator;
    private final PluginMetrics pluginMetrics;
    private final AcknowledgementSetManager acknowledgementSetManager;

    private volatile boolean shutdownRequested = false;

    public ResyncScheduler(final EnhancedSourceCoordinator sourceCoordinator,
                           final RdsSourceConfig sourceConfig,
                           final QueryManager queryManager,
                           final String s3Prefix,
                           final Buffer<Record<Event>> buffer,
                           final PluginMetrics pluginMetrics,
                           final AcknowledgementSetManager acknowledgementSetManager) {
        this.sourceCoordinator = sourceCoordinator;
        this.sourceConfig = sourceConfig;
        this.queryManager = queryManager;
        this.s3Prefix = s3Prefix;
        recordConverter = new StreamRecordConverter(s3Prefix, sourceConfig.getPartitionCount());
        bufferAccumulator = BufferAccumulator.create(buffer, DEFAULT_BUFFER_BATCH_SIZE, BUFFER_TIMEOUT);
        this.pluginMetrics = pluginMetrics;
        this.acknowledgementSetManager = acknowledgementSetManager;
    }

    @Override
    public void run() {
        LOG.debug("Start running Stream Scheduler");
        ResyncPartition resyncPartition = null;
        while (!shutdownRequested && !Thread.currentThread().isInterrupted()) {
            try {
                final Optional<EnhancedSourcePartition> sourcePartition = sourceCoordinator.acquireAvailablePartition(ResyncPartition.PARTITION_TYPE);
                if (sourcePartition.isPresent()) {
                    LOG.info("Acquired partition to perform resync");

                    resyncPartition = (ResyncPartition) sourcePartition.get();

                    processResyncPartition(resyncPartition);
                }

                try {
                    Thread.sleep(DEFAULT_TAKE_LEASE_INTERVAL_MILLIS);
                } catch (final InterruptedException e) {
                    LOG.info("The ResyncScheduler was interrupted while waiting to retry, stopping processing");
                    break;
                }

            } catch (Exception e) {
                LOG.error("Received an exception during resync, backing off and retrying", e);
                if (resyncPartition != null) {
                    sourceCoordinator.giveUpPartition(resyncPartition);
                }

                try {
                    Thread.sleep(DEFAULT_TAKE_LEASE_INTERVAL_MILLIS);
                } catch (final InterruptedException ex) {
                    LOG.info("The StreamScheduler was interrupted while waiting to retry, stopping processing");
                    break;
                }
            }
        }
    }

    public void shutdown() {
        shutdownRequested = true;
    }

    private void processResyncPartition(ResyncPartition resyncPartition) {
        String[] keySplits = resyncPartition.getPartitionKey().split("\\|");
        final String database = keySplits[0];
        final String table = keySplits[1];
        final long eventTimestampMillis = Long.parseLong(keySplits[2]);

        if (resyncPartition.getProgressState().isEmpty()) {
            final String errorMessage = "ResyncPartition " + resyncPartition.getPartitionKey() + " doesn't contain progress state.";
            throw new RuntimeException(errorMessage);
        }
        final ResyncProgressState progressState = resyncPartition.getProgressState().get();
        final String foreignKeyName = progressState.getForeignKeyName();
        final Object updatedValue = progressState.getUpdatedValue();

        LOG.debug("Will perform resync on table: {}.{}, with foreign key name: {}, and updated value: {}", database, table, foreignKeyName, updatedValue);
        String queryStatement;
        if (updatedValue == null) {
            queryStatement = String.format("SELECT * FROM %s WHERE %s IS NULL", database + "." + table, foreignKeyName);
        } else {
            queryStatement = String.format("SELECT * FROM %s WHERE %s='%s'", database + "." + table, foreignKeyName, updatedValue);
        }
        LOG.debug("Query statement: {}", queryStatement);

        List<Map<String, Object>> rows = queryManager.selectRows(queryStatement);
        LOG.debug("Found {} rows to resync", rows.size());

        for (Map<String, Object> row : rows) {
            final Event dataPrepperEvent = JacksonEvent.builder()
                    .withEventType(DATA_PREPPER_EVENT_TYPE)
                    .withData(row)
                    .build();

            final Event pipelineEvent = recordConverter.convert(
                    dataPrepperEvent,
                    database,
                    table,
                    OpenSearchBulkActions.INDEX,
                    progressState.getPrimaryKeys(),
                    eventTimestampMillis,
                    eventTimestampMillis,
                    null);

            try {
                bufferAccumulator.add(new Record<>(pipelineEvent));
            } catch (Exception e) {
                LOG.error("Failed to add event to buffer", e);
            }
        }

        try {
            bufferAccumulator.flush();
        } catch (Exception e) {
            // this will only happen if writing to buffer gets interrupted from shutdown,
            // otherwise bufferAccumulator will keep retrying with backoff
            LOG.error("Failed to flush buffer", e);
        }
    }
}
