/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.model;

public class BinlogCoordinate {
    private final String binlogFilename;
    private final long binlogPosition;

    public BinlogCoordinate(String binlogFilename, long binlogPosition) {
        this.binlogFilename = binlogFilename;
        this.binlogPosition = binlogPosition;
    }

    public String getBinlogFilename() {
        return binlogFilename;
    }

    public long getBinlogPosition() {
        return binlogPosition;
    }
}
