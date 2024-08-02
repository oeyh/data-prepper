/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.export;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.rds.converter.ExportRecordConverter;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.DataFilePartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.DataFileProgressState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class DataFileLoader implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(DataFileLoader.class);

    static final Duration VERSION_OVERLAP_TIME_FOR_EXPORT = Duration.ofMinutes(5);
    static final String EXPORT_RECORDS_TOTAL_COUNT = "exportRecordsTotal";
    static final String EXPORT_RECORDS_PROCESSED_COUNT = "exportRecordsProcessed";
    static final String EXPORT_RECORDS_PROCESSING_ERROR_COUNT = "exportRecordsProcessingErrors";
    static final String BYTES_RECEIVED = "bytesReceived";
    static final String BYTES_PROCESSED = "bytesProcessed";

    private final DataFilePartition dataFilePartition;
    private final String bucket;
    private final String objectKey;
    private final S3ObjectReader objectReader;
    private final InputCodec codec;
    private final BufferAccumulator<Record<Event>> bufferAccumulator;
    private final ExportRecordConverter recordConverter;
    private final Counter exportRecordsTotalCounter;
    private final Counter exportRecordSuccessCounter;
    private final Counter exportRecordErrorCounter;
    private final DistributionSummary bytesReceivedSummary;
    private final DistributionSummary bytesProcessedSummary;

    private DataFileLoader(final DataFilePartition dataFilePartition,
                           final InputCodec codec,
                           final BufferAccumulator<Record<Event>> bufferAccumulator,
                           final S3ObjectReader objectReader,
                           final ExportRecordConverter recordConverter,
                           final PluginMetrics pluginMetrics) {
        this.dataFilePartition = dataFilePartition;
        bucket = dataFilePartition.getBucket();
        objectKey = dataFilePartition.getKey();
        this.objectReader = objectReader;
        this.codec = codec;
        this.bufferAccumulator = bufferAccumulator;
        this.recordConverter = recordConverter;

        exportRecordsTotalCounter = pluginMetrics.counter(EXPORT_RECORDS_TOTAL_COUNT);
        exportRecordSuccessCounter = pluginMetrics.counter(EXPORT_RECORDS_PROCESSED_COUNT);
        exportRecordErrorCounter = pluginMetrics.counter(EXPORT_RECORDS_PROCESSING_ERROR_COUNT);
        bytesReceivedSummary = pluginMetrics.summary(BYTES_RECEIVED);
        bytesProcessedSummary = pluginMetrics.summary(BYTES_PROCESSED);
    }

    public static DataFileLoader create(final DataFilePartition dataFilePartition,
                                        final InputCodec codec,
                                        final BufferAccumulator<Record<Event>> bufferAccumulator,
                                        final S3ObjectReader objectReader,
                                        final ExportRecordConverter recordConverter,
                                        final PluginMetrics pluginMetrics) {
        return new DataFileLoader(dataFilePartition, codec, bufferAccumulator, objectReader, recordConverter, pluginMetrics);
    }

    @Override
    public void run() {
        LOG.info("Start loading s3://{}/{}", bucket, objectKey);

        AtomicLong eventCount = new AtomicLong();
        try (InputStream inputStream = objectReader.readFile(bucket, objectKey)) {
            codec.parse(inputStream, record -> {
                try {
                    exportRecordsTotalCounter.increment();
                    final Event event = record.getData();
                    final String string = event.toJsonString();
                    final long bytes = string.getBytes().length;
                    bytesReceivedSummary.record(bytes);

                    DataFileProgressState progressState = dataFilePartition.getProgressState().get();

                    final String fullTableName = progressState.getSourceDatabase() + "." + progressState.getSourceTable();
                    final List<String> primaryKeys = progressState.getPrimaryKeyMap().getOrDefault(fullTableName, List.of());

                    final long snapshotTime = progressState.getSnapshotTime();
                    final long eventVersionNumber = snapshotTime - VERSION_OVERLAP_TIME_FOR_EXPORT.toMillis();
                    Record<Event> transformedRecord = new Record<>(
                            recordConverter.convert(
                                    record,
                                    progressState.getSourceDatabase(),
                                    progressState.getSourceTable(),
                                    primaryKeys,
                                    snapshotTime,
                                    eventVersionNumber));
                    bufferAccumulator.add(transformedRecord);
                    eventCount.getAndIncrement();
                    bytesProcessedSummary.record(bytes);
                } catch (Exception e) {
                    LOG.error("Failed to process record from object s3://{}/{}", bucket, objectKey, e);
                    throw new RuntimeException(e);
                }
            });

            LOG.info("Completed loading object s3://{}/{} to buffer", bucket, objectKey);
        } catch (Exception e) {
            LOG.error("Failed to load object s3://{}/{} to buffer", bucket, objectKey, e);
            throw new RuntimeException(e);
        }

        try {
            bufferAccumulator.flush();
            exportRecordSuccessCounter.increment(eventCount.get());
        } catch (Exception e) {
            LOG.error("Failed to write events to buffer", e);
            exportRecordErrorCounter.increment(eventCount.get());
        }
    }
}
