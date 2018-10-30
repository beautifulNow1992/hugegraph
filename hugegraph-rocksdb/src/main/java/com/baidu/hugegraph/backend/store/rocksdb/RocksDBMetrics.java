/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.backend.store.rocksdb;

import java.util.Map;

import com.baidu.hugegraph.util.InsertionOrderUtil;

public class RocksDBMetrics {

    private static final String INDEX_FILTER =
                                "rocksdb.estimate-table-readers-mem";
    private static final String MEM_TABLE = "rocksdb.cur-size-all-mem-tables";
    private static final String DATA_SIZE = "rocksdb.estimate-live-data-size";

    public Map<String, Object> getMetrics(RocksDBSessions.Session session) {
        Map<String, Object> metrics = InsertionOrderUtil.newMap();
        metrics.put("mem_used", this.getMemUsed(session));
        metrics.put("mem_unit", "KB");
        metrics.put("data_szie", this.getDataSize(session));
        // Can put something else
        return metrics;
    }

    private long getMemUsed(RocksDBSessions.Session session) {
        long indexAndFilter = Long.parseLong(session.property(INDEX_FILTER));
        long memtable = Long.parseLong(session.property(MEM_TABLE));
        return indexAndFilter + memtable;
    }

    private long getDataSize(RocksDBSessions.Session session) {
        return Long.parseLong(session.property(DATA_SIZE));
    }
}
