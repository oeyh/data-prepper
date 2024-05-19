/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.export;

import org.opensearch.dataprepper.plugins.source.rds.model.SnapshotInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CreateDbClusterSnapshotRequest;
import software.amazon.awssdk.services.rds.model.CreateDbClusterSnapshotResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterSnapshotsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterSnapshotsResponse;

import java.time.Instant;
import java.util.UUID;

public class SnapshotManager {
    private static final Logger LOG = LoggerFactory.getLogger(SnapshotManager.class);

    private final RdsClient rdsClient;

    public SnapshotManager(final RdsClient rdsClient) {
        this.rdsClient = rdsClient;
    }

    public SnapshotInfo createDBClusterSnapshot(String dbClusterId) {
        final String snapshotId = generateSnapshotId(dbClusterId);
        CreateDbClusterSnapshotRequest request = CreateDbClusterSnapshotRequest.builder()
                .dbClusterIdentifier(dbClusterId)
                .dbClusterSnapshotIdentifier(snapshotId)
                .build();

        try {
            CreateDbClusterSnapshotResponse response = rdsClient.createDBClusterSnapshot(request);
            String snapshotArn = response.dbClusterSnapshot().dbClusterSnapshotArn();
            String status = response.dbClusterSnapshot().status();
            Instant createTime = response.dbClusterSnapshot().snapshotCreateTime();
            LOG.info("Creating snapshot with id {} and status {}", snapshotId, status);

            return new SnapshotInfo(snapshotId, snapshotArn, createTime, status);
        } catch (Exception e) {
            LOG.error("Failed to create cluster snapshot for {}", dbClusterId, e);
            return null;
        }
    }

    public SnapshotInfo checkSnapshotStatus(String snapshotId) {
        DescribeDbClusterSnapshotsRequest request = DescribeDbClusterSnapshotsRequest.builder()
                .dbClusterSnapshotIdentifier(snapshotId)
                .build();

        DescribeDbClusterSnapshotsResponse response = rdsClient.describeDBClusterSnapshots(request);
        String snapshotArn = response.dbClusterSnapshots().get(0).dbClusterSnapshotArn();
        String status = response.dbClusterSnapshots().get(0).status();
        Instant createTime = response.dbClusterSnapshots().get(0).snapshotCreateTime();

        return new SnapshotInfo(snapshotId, snapshotArn, createTime, status);
    }

    private String generateSnapshotId(String dbClusterId) {
        return dbClusterId + "-snapshot-by-pipeline-" + UUID.randomUUID().toString().substring(0, 8);
    }
}
