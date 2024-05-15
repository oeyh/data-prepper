/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.coordination.partition;

import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.ExportProgressState;

import java.time.Instant;
import java.util.Optional;

/**
 * An ExportPartition represents an export job needs to be run for tables.
 * Each export job has an export time associate with it.
 * Each job maintains the state such as total files/records etc. independently.
 * The source identifier contains keyword 'EXPORT'
 */
public class ExportPartition extends EnhancedSourcePartition<ExportProgressState> {
    public static final String PARTITION_TYPE = "EXPORT";

    private final String dbIdentifier;

    private final Instant exportTime;

    private final ExportProgressState progressState;

    public ExportPartition(String dbIdentifier, Instant exportTime) {
        this.dbIdentifier = dbIdentifier;
        this.exportTime = exportTime;
        this.progressState = null;
    }

    @Override
    public String getPartitionType() {
        return PARTITION_TYPE;
    }

    @Override
    public String getPartitionKey() {
        return dbIdentifier + "|" + exportTime.toEpochMilli();
    }

    @Override
    public Optional<ExportProgressState> getProgressState() {
        if (progressState != null) {
            return Optional.of(progressState);
        }
        return Optional.empty();
    }
}
