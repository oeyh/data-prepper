/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.model;

import java.sql.DatabaseMetaData;

public enum ForeignKeyAction {
    CASCADE,
    NO_ACTION,
    RESTRICT,
    SET_DEFAULT,
    SET_NULL,
    UNKNOWN;

    public static ForeignKeyAction getActionFromMetadata(short action) {
        switch (action) {
            case DatabaseMetaData.importedKeyCascade:
                return CASCADE;
            case DatabaseMetaData.importedKeySetNull:
                return SET_NULL;
            case DatabaseMetaData.importedKeySetDefault:
                return SET_DEFAULT;
            case DatabaseMetaData.importedKeyRestrict:
                return RESTRICT;
            case DatabaseMetaData.importedKeyNoAction:
                return NO_ACTION;
            default:
                return UNKNOWN;
        }
    }
}
