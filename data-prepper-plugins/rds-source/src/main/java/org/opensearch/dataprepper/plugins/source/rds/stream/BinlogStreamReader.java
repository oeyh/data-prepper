/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class BinlogStreamReader {

    private static final Logger LOG = LoggerFactory.getLogger(BinlogStreamReader.class);

    private BinaryLogClient binaryLogClient;

    public BinlogStreamReader(BinaryLogClient binaryLogClient) {
        this.binaryLogClient = binaryLogClient;
    }

    public void start() {
        try {
            binaryLogClient.connect();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
