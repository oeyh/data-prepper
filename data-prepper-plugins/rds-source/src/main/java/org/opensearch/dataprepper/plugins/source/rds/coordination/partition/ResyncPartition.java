/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.coordination.partition;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.ResyncProgressState;

import java.util.Optional;

public class ResyncPartition extends EnhancedSourcePartition<ResyncProgressState> {

    public static final String PARTITION_TYPE = "RESYNC";

    private final String dbIdentifier;
    private final ResyncProgressState state;

    public ResyncPartition(String dbIdentifier, ResyncProgressState state) {
        this.dbIdentifier = dbIdentifier;
        this.state = state;
    }

    public ResyncPartition(SourcePartitionStoreItem sourcePartitionStoreItem) {
        setSourcePartitionStoreItem(sourcePartitionStoreItem);
        dbIdentifier = sourcePartitionStoreItem.getSourcePartitionKey();
        state = convertStringToPartitionProgressState(ResyncProgressState.class, sourcePartitionStoreItem.getPartitionProgressState());
    }

    @Override
    public String getPartitionType() {
        return PARTITION_TYPE;
    }

    @Override
    public String getPartitionKey() {
        return dbIdentifier;
    }

    @Override
    public Optional<ResyncProgressState> getProgressState() {
        if (state != null) {
            return Optional.of(state);
        }
        return Optional.empty();
    }
}
