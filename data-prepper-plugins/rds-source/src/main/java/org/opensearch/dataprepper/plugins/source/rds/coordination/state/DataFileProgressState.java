/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.coordination.state;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DataFileProgressState {


    @JsonProperty("totalRecords")
    private int total;

    @JsonProperty("loadedRecords")
    private int loaded;

    @JsonProperty("exportStartTime")
    private long startTime;

    public int getTotal() {
        return total;
    }

    public int getLoaded() {
        return loaded;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public void setLoaded(int loaded) {
        this.loaded = loaded;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getStartTime() {
        return startTime;
    }
}
