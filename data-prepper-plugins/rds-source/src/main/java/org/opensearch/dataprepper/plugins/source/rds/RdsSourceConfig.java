/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.plugins.source.rds.configuration.AwsAuthenticationConfig;

import java.util.List;

/**
 * Configuration for RDS Source
 */
public class RdsSourceConfig {

    @JsonProperty("db_identifier")
    private String dbIdentifier;

    @JsonProperty("tables")
    private List<String> tables;

    @JsonProperty("aws")
    @NotNull
    @Valid
    private AwsAuthenticationConfig awsAuthenticationConfig;

    @JsonProperty("acknowledgments")
    private boolean acknowledgments = false;

    @JsonProperty("s3_bucket")
    private String s3Bucket;

    @JsonProperty("s3_prefix")
    private String s3Prefix;

    @JsonProperty("s3_region")
    private String s3Region;

    @JsonProperty("export")
    private boolean isExport;

    public String getDbIdentifier() {
        return dbIdentifier;
    }

    public List<String> getTables() {
        return tables;
    }

    public AwsAuthenticationConfig getAwsAuthenticationConfig() {
        return awsAuthenticationConfig;
    }

    public boolean isAcknowledgmentsEnabled() {
        return this.acknowledgments;
    }

    public String getS3Bucket() {
        return this.s3Bucket;
    }

    public String getS3Prefix() {
        return this.s3Prefix;
    }

    public String getS3Region() {
        return this.s3Region;
    }
}
