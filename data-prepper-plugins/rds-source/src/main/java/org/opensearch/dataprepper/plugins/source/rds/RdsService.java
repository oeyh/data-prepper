/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.rds.export.DataFileScheduler;
import org.opensearch.dataprepper.plugins.source.rds.export.ExportScheduler;
import org.opensearch.dataprepper.plugins.source.rds.leader.LeaderScheduler;
import org.opensearch.dataprepper.plugins.source.rds.stream.StreamScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RdsService {
    private static final Logger LOG = LoggerFactory.getLogger(RdsService.class);

    private final RdsClient rdsClient;
    private final S3Client s3Client;
    private final EnhancedSourceCoordinator sourceCoordinator;
    private final EventFactory eventFactory;
    private final PluginMetrics pluginMetrics;
    private final RdsSourceConfig sourceConfig;
    private final ExecutorService executor;

    public RdsService(final EnhancedSourceCoordinator sourceCoordinator,
                      final RdsSourceConfig sourceConfig,
                      final EventFactory eventFactory,
                      final ClientFactory clientFactory,
                      final PluginMetrics pluginMetrics) {
        this.sourceCoordinator = sourceCoordinator;
        this.eventFactory = eventFactory;
        this.pluginMetrics = pluginMetrics;
        this.sourceConfig = sourceConfig;

        rdsClient = clientFactory.buildRdsClient();
        s3Client = clientFactory.buildS3Client();
        executor = Executors.newFixedThreadPool(4);
    }

    /**
     * This service start three long-running threads (scheduler)
     * Each thread is responsible for one type of job.
     * The data will be guaranteed to be sent to {@link Buffer} in order.
     *
     * @param buffer Data Prepper Buffer
     */
    public void start(Buffer<Record<Event>> buffer) {
        LOG.info("Start running RDS service");
        Runnable leaderScheduler = new LeaderScheduler(sourceCoordinator, sourceConfig);
        Runnable exportScheduler = new ExportScheduler(sourceCoordinator, rdsClient, s3Client, pluginMetrics);
        Runnable dataFileScheduler = new DataFileScheduler(sourceCoordinator, sourceConfig, s3Client, eventFactory, buffer);
        Runnable streamScheduler = new StreamScheduler();

        executor.submit(leaderScheduler);
        executor.submit(exportScheduler);
        executor.submit(dataFileScheduler);
        executor.submit(streamScheduler);
    }

    /**
     * Interrupt the running of schedulers.
     * Each scheduler must implement logic for gracefully shutdown.
     */
    public void shutdown() {
        LOG.info("shutdown RDS schedulers");
        executor.shutdownNow();
    }
}
