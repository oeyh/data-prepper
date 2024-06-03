/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.rds.converter.S3PartitionCreator;
import org.opensearch.dataprepper.plugins.source.rds.converter.StreamRecordConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BinlogEventListener implements BinaryLogClient.EventListener {

    private static final Logger LOG = LoggerFactory.getLogger(BinlogEventListener.class);

    static final Duration BUFFER_TIMEOUT = Duration.ofSeconds(60);
    static final int DEFAULT_BUFFER_BATCH_SIZE = 1_000;

    private final Map<Long, String> tableNameMap;
    private final Map<Long, List<String>> columnNamesMap;
    private final StreamRecordConverter recordConverter;
    private final BufferAccumulator<Record<Event>> bufferAccumulator;
    private final String s3Prefix;

    public BinlogEventListener(final Buffer<Record<Event>> buffer, final String s3Prefix) {
        tableNameMap = new HashMap<>();
        columnNamesMap = new HashMap<>();
        recordConverter = new StreamRecordConverter();
        bufferAccumulator = BufferAccumulator.create(buffer, DEFAULT_BUFFER_BATCH_SIZE, BUFFER_TIMEOUT);
        this.s3Prefix = s3Prefix;
    }

    @Override
    public void onEvent(com.github.shyiko.mysql.binlog.event.Event event) {
        EventType eventType = event.getHeader().getEventType();

        switch (eventType) {
            case TABLE_MAP:
                handleTableMapEvent(event);
                break;
            case WRITE_ROWS:
            case EXT_WRITE_ROWS:
                handleInsertEvent(event);
                break;
        }
    }

    private void handleTableMapEvent(com.github.shyiko.mysql.binlog.event.Event event) {
        TableMapEventData data = event.getData();
        String databaseName = data.getDatabase();
        String tableName = data.getTable();
        String fullTableName = databaseName + "." + tableName;
        List<String> columnNames = data.getEventMetadata().getColumnNames();
        tableNameMap.put(data.getTableId(), fullTableName);
        columnNamesMap.put(data.getTableId(), columnNames);
    }
    private void handleInsertEvent(com.github.shyiko.mysql.binlog.event.Event event) {
        // get new row data from the event
        LOG.debug("Handling insert event");
        WriteRowsEventData data = event.getData();

        // Construct data prepper JacksonEvent
        for (Object[] rowDataArray : data.getRows()) {
            String tableName = tableNameMap.get(data.getTableId());
            List<String> columnNames = columnNamesMap.get(data.getTableId());
            Map<String, Object> rowDataMap = new HashMap<>();
            for (int i = 0; i < rowDataArray.length; i++) {
                rowDataMap.put(columnNames.get(i), rowDataArray[i]);
            }

            // TODO: the primary key name should be from querying the database schema
            final String primaryKey = columnNames.get(0);
            Event pipelineEvent = recordConverter.convert(rowDataMap, tableName, "index", primaryKey, s3Prefix);
            try {
                bufferAccumulator.add(new Record<>(pipelineEvent));
            } catch (Exception e) {
                LOG.error("Failed to add event to buffer", e);
            }
        }

        try {
            bufferAccumulator.flush();
        } catch (Exception e) {
            LOG.error("Failed to flush buffer", e);
        }
    }
}
