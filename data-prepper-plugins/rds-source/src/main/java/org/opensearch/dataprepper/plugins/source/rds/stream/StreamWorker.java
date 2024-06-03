/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.rds.converter.S3PartitionCreator;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.StreamProgressState;
import org.opensearch.dataprepper.plugins.source.rds.model.BinlogCoordinate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

public class StreamWorker {
    private static final Logger LOG = LoggerFactory.getLogger(StreamWorker.class);

    private static final int DEFAULT_EXPORT_COMPLETE_WAIT_INTERVAL_MILLIS = 60_000;

    private final EnhancedSourceCoordinator sourceCoordinator;
    private final BinaryLogClient binaryLogClient;
    private final PluginMetrics pluginMetrics;

    public StreamWorker(final EnhancedSourceCoordinator sourceCoordinator,
                        final BinaryLogClient binaryLogClient,
                        final PluginMetrics pluginMetrics) {
        this.sourceCoordinator = sourceCoordinator;
        this.binaryLogClient = binaryLogClient;
        this.pluginMetrics = pluginMetrics;
    }

    public void processStream(final StreamPartition streamPartition) {
        // get current binlog position
        Optional<BinlogCoordinate> currentBinlogCoords = streamPartition.getProgressState().map(StreamProgressState::getCurrentPosition);

        // set start of binlog stream to current position if exists
        if (currentBinlogCoords.isPresent()) {
            final String binlogFilename = currentBinlogCoords.get().getBinlogFilename();
            final long binlogPosition = currentBinlogCoords.get().getBinlogPosition();
            LOG.debug("Will start binlog stream from binlog file {} and position {}.", binlogFilename, binlogPosition);
            binaryLogClient.setBinlogFilename(binlogFilename);
            binaryLogClient.setBinlogPosition(binlogPosition);
        }

        while (shouldWaitForExport(streamPartition) && !Thread.currentThread().isInterrupted()) {
            LOG.info("Initial load not completed yet for {}, waiting...", streamPartition.getPartitionKey());
            try {
                Thread.sleep(DEFAULT_EXPORT_COMPLETE_WAIT_INTERVAL_MILLIS);
            } catch (final InterruptedException ex) {
                LOG.info("The StreamScheduler was interrupted while waiting to retry, stopping processing");
                Thread.currentThread().interrupt();
                break;
            }
        }

        try {
            LOG.info("Connecting to binary log stream.");
            binaryLogClient.connect();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                binaryLogClient.disconnect();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private boolean shouldWaitForExport(final StreamPartition streamPartition) {
        if (!streamPartition.getProgressState().get().shouldWaitForExport()) {
            LOG.debug("Export is not enabled. Proceed with streaming.");
            return false;
        }

        return !isExportDone(streamPartition);
    }

    private boolean isExportDone(StreamPartition streamPartition) {
        final String dbIdentifier = streamPartition.getPartitionKey();
        Optional<EnhancedSourcePartition> globalStatePartition = sourceCoordinator.getPartition("stream-for-" + dbIdentifier);
        return globalStatePartition.isPresent();
    }
}
