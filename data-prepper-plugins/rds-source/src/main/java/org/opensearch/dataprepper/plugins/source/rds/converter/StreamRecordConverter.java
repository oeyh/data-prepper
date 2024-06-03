/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.converter;

import org.opensearch.dataprepper.model.event.Event;

import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;

import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.EVENT_TABLE_NAME_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE;

/**
 * Convert binlog row data into JacksonEvent
 */
public class StreamRecordConverter {

    private static final Logger LOG = LoggerFactory.getLogger(StreamRecordConverter.class);
    // TODO: this will be added to source config
    private static final int S3_FOLDER_PARTITION_COUNT = 20;
    private static final String S3_PATH_DELIMITER = "/";

    private final List<String> folderNames;
    public StreamRecordConverter() {
        S3PartitionCreator s3PartitionCreator = new S3PartitionCreator(S3_FOLDER_PARTITION_COUNT);
        folderNames = s3PartitionCreator.createPartition();
    }

    public Event convert(Map<String, Object> rowData, String tableName, String bulkAction, String primaryKeyName, String s3Prefix) {
        final Event event = JacksonEvent.builder()
                .withEventType("STREAM")
                .withData(rowData)
                .build();

        EventMetadata eventMetadata = event.getMetadata();

        eventMetadata.setAttribute(EVENT_TABLE_NAME_METADATA_ATTRIBUTE, tableName);
        eventMetadata.setAttribute(EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE, bulkAction);

        final Object primaryKeyValue = rowData.get(primaryKeyName);
        eventMetadata.setAttribute(PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE, primaryKeyValue);
        eventMetadata.setAttribute(MetadataKeyAttributes.EVENT_S3_PARTITION_KEY, s3Prefix + S3_PATH_DELIMITER + hashKeyToPartition(primaryKeyValue.toString()));

        return event;
    }

    private String hashKeyToPartition(final String key) {
        return folderNames.get(hashKeyToIndex(key));
    }
    private int hashKeyToIndex(final String key) {
        try {
            // Create a SHA-256 hash instance
            final MessageDigest digest = MessageDigest.getInstance("SHA-256");
            // Hash the key
            byte[] hashBytes = digest.digest(key.getBytes());
            // Convert the hash to an integer
            int hashValue = bytesToInt(hashBytes);
            // Map the hash value to an index in the list
            return Math.abs(hashValue) % folderNames.size();
        } catch (final NoSuchAlgorithmException
                e) {
            return -1;
        }
    }
    private static int bytesToInt(byte[] bytes) {
        int result = 0;
        for (int i = 0; i < 4 && i < bytes.length; i++) {
            result <<= 8;
            result |= (bytes[i] & 0xFF);
        }
        return result;
    }
}
