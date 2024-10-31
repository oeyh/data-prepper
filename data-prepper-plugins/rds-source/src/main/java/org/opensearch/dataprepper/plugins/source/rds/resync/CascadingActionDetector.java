/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.resync;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.ResyncPartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.ResyncProgressState;
import org.opensearch.dataprepper.plugins.source.rds.model.ForeignKeyAction;
import org.opensearch.dataprepper.plugins.source.rds.model.ForeignKeyRelation;
import org.opensearch.dataprepper.plugins.source.rds.model.ParentTable;
import org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CascadingActionDetector {

    private static final Logger LOG = LoggerFactory.getLogger(CascadingActionDetector.class);

    private final EnhancedSourceCoordinator sourceCoordinator;

    public CascadingActionDetector(final EnhancedSourceCoordinator sourceCoordinator) {
        this.sourceCoordinator = sourceCoordinator;
    }

    public void detectCascadingUpdates(Event event, Map<String, ParentTable> parentTableMap, TableMetadata tableMetadata) {
        final UpdateRowsEventData data = event.getData();
        if (parentTableMap.containsKey(tableMetadata.getFullTableName())) {
            final ParentTable parentTable = parentTableMap.get(tableMetadata.getFullTableName());

            for (Map.Entry<Serializable[], Serializable[]> row : data.getRows()) {
                // Find out for this row, which columns are changing
                LOG.debug("Checking for updated columns");
                final Map<String, Object> updatedColumnsAndValues = IntStream.range(0, row.getKey().length)
                        .filter(i -> !row.getKey()[i].equals(row.getValue()[i]))
                        .mapToObj(i -> tableMetadata.getColumnNames().get(i))
                        .collect(Collectors.toMap(
                                column -> column,
                                column -> row.getValue()[tableMetadata.getColumnNames().indexOf(column)]
                        ));
                LOG.debug("These columns were updated: {}", updatedColumnsAndValues);

                LOG.debug("Decide whether to create resync partitions");
                // Create resync partition if changing columns are associated with cascading update
                for (String column : updatedColumnsAndValues.keySet()) {
                    if (parentTable.getColumnsWithCascadingUpdate().containsKey(column)) {
                        for (ForeignKeyRelation foreignKeyRelation : parentTable.getColumnsWithCascadingUpdate().get(column)) {
                            if (foreignKeyRelation.getUpdateAction() == ForeignKeyAction.CASCADE) {
                                createResyncPartition(
                                        foreignKeyRelation.getDatabaseName(),
                                        foreignKeyRelation.getChildTableName(),
                                        foreignKeyRelation.getForeignKeyName(),
                                        updatedColumnsAndValues.get(column),
                                        tableMetadata.getPrimaryKeys(),
                                        event.getHeader().getTimestamp());
                            } else if (foreignKeyRelation.getUpdateAction() == ForeignKeyAction.SET_NULL) {
                                createResyncPartition(
                                        foreignKeyRelation.getDatabaseName(),
                                        foreignKeyRelation.getChildTableName(),
                                        foreignKeyRelation.getForeignKeyName(),
                                        null,
                                        tableMetadata.getPrimaryKeys(),
                                        event.getHeader().getTimestamp());
                            } else if (foreignKeyRelation.getUpdateAction() == ForeignKeyAction.SET_DEFAULT) {
                                createResyncPartition(
                                        foreignKeyRelation.getDatabaseName(),
                                        foreignKeyRelation.getChildTableName(),
                                        foreignKeyRelation.getForeignKeyName(),
                                        foreignKeyRelation.getForeignKeyDefaultValue(),
                                        tableMetadata.getPrimaryKeys(),
                                        event.getHeader().getTimestamp());
                            }
                        }
                    }
                }
            }
        }
    }

    public void detectCascadingDeletes(Event event, Map<String, ParentTable> parentTableMap, TableMetadata tableMetadata) {
        if (parentTableMap.containsKey(tableMetadata.getFullTableName())) {
            final ParentTable parentTable = parentTableMap.get(tableMetadata.getFullTableName());

            for (String column : parentTable.getColumnsWithCascadingDelete().keySet()) {
                for (ForeignKeyRelation foreignKeyRelation : parentTable.getColumnsWithCascadingDelete().get(column)) {
                    if (foreignKeyRelation.getDeleteAction() == ForeignKeyAction.CASCADE) {
                        LOG.warn("Cascade delete is not supported yet");
                    } else if (foreignKeyRelation.getDeleteAction() == ForeignKeyAction.SET_NULL) {
                        // foreign key in the child table will be set to NULL
                        createResyncPartition(
                                foreignKeyRelation.getDatabaseName(),
                                foreignKeyRelation.getChildTableName(),
                                foreignKeyRelation.getForeignKeyName(),
                                null,
                                tableMetadata.getPrimaryKeys(),
                                event.getHeader().getTimestamp());
                    } else if (foreignKeyRelation.getDeleteAction() == ForeignKeyAction.SET_DEFAULT) {
                        createResyncPartition(
                                foreignKeyRelation.getDatabaseName(),
                                foreignKeyRelation.getChildTableName(),
                                foreignKeyRelation.getForeignKeyName(),
                                foreignKeyRelation.getForeignKeyDefaultValue(),
                                tableMetadata.getPrimaryKeys(),
                                event.getHeader().getTimestamp());
                    }
                }
            }
        }
    }

    private void createResyncPartition(String database, String childTable, String foreignKeyName, Object updatedValue, List<String> primaryKeys, long eventTimestampMillis) {
        LOG.debug("Create Resyc partition for table {} and column {} with new value {}", childTable, foreignKeyName, updatedValue);
        final ResyncProgressState progressState = new ResyncProgressState();
        progressState.setForeignKeyName(foreignKeyName);
        progressState.setUpdatedValue(updatedValue);
        progressState.setPrimaryKeys(primaryKeys);

        final ResyncPartition resyncPartition = new ResyncPartition(database, childTable, eventTimestampMillis, progressState);
        sourceCoordinator.createPartition(resyncPartition);
    }
}
