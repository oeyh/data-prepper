/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.StreamProgressState;
import org.opensearch.dataprepper.plugins.source.rds.model.BinlogCoordinate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

public class StreamWorker {
    private static final Logger LOG = LoggerFactory.getLogger(StreamWorker.class);

    private final BinaryLogClient binaryLogClient;
    private final PluginMetrics pluginMetrics;

    public StreamWorker(final BinaryLogClient binaryLogClient, final PluginMetrics pluginMetrics) {
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
}
