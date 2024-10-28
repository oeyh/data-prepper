/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.schema;

import org.opensearch.dataprepper.plugins.source.rds.model.BinlogCoordinate;
import org.opensearch.dataprepper.plugins.source.rds.model.ForeignKeyAction;
import org.opensearch.dataprepper.plugins.source.rds.model.ForeignKeyRelation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class SchemaManager {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaManager.class);
    static final String COLUMN_NAME = "COLUMN_NAME";
    static final String BINLOG_STATUS_QUERY = "SHOW MASTER STATUS";
    static final String BINLOG_FILE = "File";
    static final String BINLOG_POSITION = "Position";
    static final int NUM_OF_RETRIES = 3;
    static final int BACKOFF_IN_MILLIS = 500;
    public static final String FKTABLE_NAME = "FKTABLE_NAME";
    public static final String FKCOLUMN_NAME = "FKCOLUMN_NAME";
    public static final String PKTABLE_NAME = "PKTABLE_NAME";
    public static final String PKCOLUMN_NAME = "PKCOLUMN_NAME";
    public static final String UPDATE_RULE = "UPDATE_RULE";
    public static final String DELETE_RULE = "DELETE_RULE";
    private final ConnectionManager connectionManager;

    public SchemaManager(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    public List<String> getPrimaryKeys(final String database, final String table) {
        int retry = 0;
        while (retry <= NUM_OF_RETRIES) {
            final List<String> primaryKeys = new ArrayList<>();
            try (final Connection connection = connectionManager.getConnection()) {
                final ResultSet rs = connection.getMetaData().getPrimaryKeys(database, null, table);
                while (rs.next()) {
                    primaryKeys.add(rs.getString(COLUMN_NAME));
                }
                return primaryKeys;
            } catch (Exception e) {
                LOG.error("Failed to get primary keys for table {}, retrying", table, e);
            }
            applyBackoff();
            retry++;
        }
        LOG.warn("Failed to get primary keys for table {}", table);
        return List.of();
    }

    public Optional<BinlogCoordinate> getCurrentBinaryLogPosition() {
        int retry = 0;
        while (retry <= NUM_OF_RETRIES) {
            try (final Connection connection = connectionManager.getConnection()) {
                final Statement statement = connection.createStatement();
                final ResultSet rs = statement.executeQuery(BINLOG_STATUS_QUERY);
                if (rs.next()) {
                    return Optional.of(new BinlogCoordinate(rs.getString(BINLOG_FILE), rs.getLong(BINLOG_POSITION)));
                }
            } catch (Exception e) {
                LOG.error("Failed to get current binary log position, retrying", e);
            }
            applyBackoff();
            retry++;
        }
        LOG.warn("Failed to get current binary log position");
        return Optional.empty();
    }

    /**
     * Get the foreign key relations associated with the given tables.
     *
     * @param tableNames the table names
     * @return the foreign key relations
     */
    public List<ForeignKeyRelation> getForeignKeyRelations(List<String> tableNames) {
        int retry = 0;
        while (retry <= NUM_OF_RETRIES) {
            try (final Connection connection = connectionManager.getConnection()) {
                final List<ForeignKeyRelation> foreignKeyRelations = new ArrayList<>();
                DatabaseMetaData metaData = connection.getMetaData();
                String[] tableTypes = new String[]{"TABLE"};
                for (final String tableName : tableNames) {
                    String database = tableName.split("\\.")[0];
                    String table = tableName.split("\\.")[1];
                    ResultSet tableResult = metaData.getTables(database, null, table, tableTypes);
                    while (tableResult.next()) {
                        ResultSet foreignKeys = metaData.getImportedKeys(database, null, table);

                        while (foreignKeys.next()) {
                            String fkTableName = foreignKeys.getString(FKTABLE_NAME);
                            String fkColumnName = foreignKeys.getString(FKCOLUMN_NAME);
                            String pkTableName = foreignKeys.getString(PKTABLE_NAME);
                            String pkColumnName = foreignKeys.getString(PKCOLUMN_NAME);
                            short updateRule = foreignKeys.getShort(UPDATE_RULE);
                            short deleteRule = foreignKeys.getShort(DELETE_RULE);

                            ForeignKeyRelation foreignKeyRelation = ForeignKeyRelation.builder()
                                    .databaseName(database)
                                    .parentTableName(pkTableName)
                                    .referencedKeyName(pkColumnName)
                                    .childTableName(fkTableName)
                                    .foreignKeyName(fkColumnName)
                                    .updateAction(ForeignKeyAction.getActionFromMetadata(updateRule))
                                    .deleteAction(ForeignKeyAction.getActionFromMetadata(deleteRule))
                                    .build();

                            foreignKeyRelations.add(foreignKeyRelation);
                        }
                    }
                }

                return foreignKeyRelations;
            } catch (Exception e) {
                LOG.error("Failed to scan foreign key references, retrying", e);
            }
            applyBackoff();
            retry++;
        }
        LOG.warn("Failed to scan foreign key references");
        return List.of();
    }

    private void applyBackoff() {
        try {
            Thread.sleep(BACKOFF_IN_MILLIS);
        } catch (final InterruptedException e){
            Thread.currentThread().interrupt();
        }
    }
}
