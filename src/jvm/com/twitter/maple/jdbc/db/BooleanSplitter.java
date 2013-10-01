/*
 * Copyright (c) 2009 Concurrent, Inc.
 *
 * This work has been released into the public domain
 * by the copyright holder. This applies worldwide.
 *
 * In case this is not legally possible:
 * The copyright holder grants any entity the right
 * to use this work for any purpose, without any
 * conditions, unless such conditions are required by law.
 */
/**
 * Copyright 2012 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.maple.jdbc.db;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;

/**
 * Implement DBSplitter over boolean values.
 */
public class BooleanSplitter implements DBSplitter {
    public List<InputSplit> split(Configuration conf, ResultSet results, String colName)
            throws SQLException {

        List<InputSplit> splits = new ArrayList<InputSplit>();

        if (results.getString(1) == null && results.getString(2) == null) {
            // Range is null to null. Return a null split accordingly.
            splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
                    colName + " IS NULL", colName + " IS NULL"));
            return splits;
        }

        boolean minVal = results.getBoolean(1);
        boolean maxVal = results.getBoolean(2);

        // Use one or two splits.
        if (!minVal) {
            splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
                    colName + " = FALSE", colName + " = FALSE"));
        }

        if (maxVal) {
            splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
                    colName + " = TRUE", colName + " = TRUE"));
        }

        if (results.getString(1) == null || results.getString(2) == null) {
            // Include a null value.
            splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
                    colName + " IS NULL", colName + " IS NULL"));
        }

        return splits;
    }
}
