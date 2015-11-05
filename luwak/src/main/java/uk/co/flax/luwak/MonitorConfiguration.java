package uk.co.flax.luwak;

/*
 *   Copyright (c) 2015 Lemur Consulting Ltd.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

import java.util.concurrent.TimeUnit;

/**
 * Encapsulates various configuration settings for a Monitor
 */
public class MonitorConfiguration {

    private int queryUpdateBufferSize = 5000;
    private long purgeFrequency = 5;
    private TimeUnit purgeFrequencyUnits = TimeUnit.MINUTES;
    private int slowLogLimit = 2000000;
    private QueryDecomposer queryDecomposer = new QueryDecomposer();

    /**
     * Set the QueryDecomposer to be used by the Monitor
     */
    public MonitorConfiguration setQueryDecomposer(QueryDecomposer queryDecomposer) {
        this.queryDecomposer = queryDecomposer;
        return this;
    }

    /**
     * @return the QueryDecomposer to be used by the Monitor
     */
    public QueryDecomposer getQueryDecomposer() {
        return queryDecomposer;
    }

    /**
     * Set the frequency with with the Monitor's querycache will be garbage-collected
     * @param frequency the frequency value
     * @param units     the frequency units
     */
    public MonitorConfiguration setPurgeFrequency(long frequency, TimeUnit units) {
        this.purgeFrequency = frequency;
        this.purgeFrequencyUnits = units;
        return this;
    }

    /**
     * Get the value of Monitor's querycache garbage-collection frequency
     */
    public long getPurgeFrequency() {
        return purgeFrequency;
    }

    /**
     * Get the units of the Monitor's querycache garbage-collection frequency
     */
    public TimeUnit getPurgeFrequencyUnits() {
        return purgeFrequencyUnits;
    }

    /**
     * Set how many queries will be buffered in memory before being committed to the queryindex
     */
    public MonitorConfiguration setQueryUpdateBufferSize(int size) {
        this.queryUpdateBufferSize = size;
        return this;
    }

    /**
     * Get the size of the queryindex's in-memory buffer
     */
    public int getQueryUpdateBufferSize() {
        return queryUpdateBufferSize;
    }

    /**
     * Set the initial slow log limit of this Monitor
     *
     * @see SlowLog
     */
    public MonitorConfiguration setSlowLogLimit(int limit) {
        this.slowLogLimit = limit;
        return this;
    }

    /**
     * Get the slow log limit
     */
    public int getSlowLogLimit() {
        return slowLogLimit;
    }
}
