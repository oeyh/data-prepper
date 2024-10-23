/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.model;

import lombok.Builder;
import lombok.Getter;

import java.util.List;
import java.util.Map;

/**
 * A data model for a parent table in a foreign key relationship
 */
@Getter
@Builder
public class ParentTable {
    private final String databaseName;
    private final String tableName;
    /**
     * Column name to a list of ForeignKeyRelation in which the column is referenced
     */
    private final Map<String, List<ForeignKeyRelation>> referencedColumnMap;
}
