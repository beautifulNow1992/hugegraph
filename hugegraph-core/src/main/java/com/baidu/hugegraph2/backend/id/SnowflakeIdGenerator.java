package com.baidu.hugegraph2.backend.id;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import src.main.java.com.baidu.hugegraph2.HugeException;
import src.main.java.com.baidu.hugegraph2.util.TimeUtil;

public class SnowflakeIdGenerator extends IdGenerator {

    private static IdWorker idWorker = null;

    public static void init(long workerId, long datacenterId) {
        idWorker = new IdWorker(workerId, datacenterId);
    }

    public static Id generate() {
        if (idWorker == null) {
            throw new HugeException("Please initialize before using it");
        }
        return generate(idWorker.nextId());
    }

    /*
     * Copyright 2010-2012 Twitter, Inc.
     * Licensed under the Apache License, Version 2.0 (the "License");
     * you may not use this file except in compliance with the License.
     * You may obtain a copy of the License at
     *      http://www.apache.org/licenses/LICENSE-2.0
     * Unless required by applicable law or agreed to in writing,
     * software distributed under the License is distributed on an "AS IS" BASIS,
     * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     * See the License for the specific language governing permissions and
     * limitations under the License.
     */
    static class IdWorker {

        protected static final Logger LOG = LoggerFactory.getLogger(IdWorker.class);

        private long workerId;
        private long datacenterId;
        private long sequence = 0L;

        private long workerIdBits = 5L;
        private long datacenterIdBits = 5L;
        private long maxWorkerId = -1L ^ (-1L << this.workerIdBits);
        private long maxDatacenterId = -1L ^ (-1L << this.datacenterIdBits);
        private long sequenceBits = 12L;

        private long workerIdShift = this.sequenceBits;
        private long datacenterIdShift = this.sequenceBits + this.workerIdBits;
        private long timestampLeftShift = this.sequenceBits + this.workerIdBits + this.datacenterIdBits;
        private long sequenceMask = -1L ^ (-1L << this.sequenceBits);

        private long lastTimestamp = -1L;

        public IdWorker(long workerId, long datacenterId) {
            // sanity check for workerId
            if (workerId > this.maxWorkerId || workerId < 0) {
                throw new IllegalArgumentException(String.format(
                        "worker Id can't be greater than %d or less than 0",
                        this.maxWorkerId));
            }
            if (datacenterId > this.maxDatacenterId || datacenterId < 0) {
                throw new IllegalArgumentException(String.format(
                        "datacenter Id can't be greater than %d or less than 0",
                        this.maxDatacenterId));
            }
            this.workerId = workerId;
            this.datacenterId = datacenterId;
            LOG.info(String.format(
                    "worker starting. timestamp left shift %d,"
                    + "datacenter id bits %d, worker id bits %d,"
                    + "sequence bits %d, workerid %d",
                    this.timestampLeftShift,
                    this.datacenterIdBits,
                    this.workerIdBits,
                    this.sequenceBits,
                    workerId));
        }

        public synchronized long nextId() {
            long timestamp = TimeUtil.timeGen();

            if (timestamp < this.lastTimestamp) {
                LOG.error(String.format("clock is moving backwards."
                        + "Rejecting requests until %d.",
                        this.lastTimestamp));
                throw new RuntimeException(String.format("Clock moved backwards."
                        + "Refusing to generate id for %d milliseconds",
                        this.lastTimestamp - timestamp));
            }

            if (this.lastTimestamp == timestamp) {
                this.sequence = (this.sequence + 1) & this.sequenceMask;
                if (this.sequence == 0) {
                    timestamp = TimeUtil.tilNextMillis(this.lastTimestamp);
                }
            } else {
                this.sequence = 0L;
            }

            this.lastTimestamp = timestamp;

            return (timestamp << this.timestampLeftShift)
                    | (this.datacenterId << this.datacenterIdShift)
                    | (this.workerId << this.workerIdShift)
                    | this.sequence;
        }

    }
}