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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;

/**
 * DBSplitter will generate DBInputSplits to use with DataDrivenDBInputFormat.
 * DataDrivenDBInputFormat needs to interpolate between two values that
 * represent the lowest and highest valued records to import. Depending
 * on the data-type of the column, this requires different behavior.
 * DBSplitter implementations should perform this for a data type or family
 * of data types.
 */
public interface DBSplitter {
    /**
     * Given a ResultSet containing one record (and already advanced to that record)
     * with two columns (a low value, and a high value, both of the same type), determine
     * a set of splits that span the given values.
     */
    List<InputSplit> split(Configuration conf, ResultSet results, String colName) throws SQLException;
}
