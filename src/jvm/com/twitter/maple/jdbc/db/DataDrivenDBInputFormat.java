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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * A InputFormat that reads input data from an SQL table.
 * Operates like DBInputFormat, but instead of using LIMIT and OFFSET to demarcate
 * splits, it tries to generate WHERE clauses which separate the data into roughly
 * equivalent shards.
 */
public class DataDrivenDBInputFormat<T extends DBWritable> extends DBInputFormat<T> {

    /** Field LOG */
    private static final Logger LOG = LoggerFactory.getLogger(DataDrivenDBInputFormat.class);

    /**
     * A RecordReader that reads records from a SQL table,
     * using data-driven WHERE clause splits.
     * Emits LongWritables containing the record number as
     * key and DBWritables as value.
     */
    public class DataDrivenDBRecordReader implements RecordReader<LongWritable, T> {
        private ResultSet results;
        private Statement statement;
        private Class<T> inputClass;
        private JobConf job;
        private DataDrivenDBInputSplit split;
        private long pos = 0;

        /**
         * @param split The InputSplit to read data for
         * @throws SQLException
         */
        protected DataDrivenDBRecordReader(DataDrivenDBInputSplit split, Class<T> inputClass, JobConf job)
            throws SQLException, IOException {
            this.inputClass = inputClass;
            this.split = split;
            this.job = job;

            statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

            //statement.setFetchSize(Integer.MIN_VALUE);
            String query = getSelectQuery();
            try {
                LOG.info(query);
                results = statement.executeQuery(query);
                LOG.info("done executing select query");
            } catch (SQLException exception) {
                LOG.error("unable to execute select query: " + query, exception);
                throw new IOException("unable to execute select query: " + query, exception);
            }
        }

        protected String getSelectQuery() {
            LOG.info("Executing select query");
            StringBuilder query = new StringBuilder();

            // Build the WHERE clauses associated with the data split first.
            // We need them in both branches of this function.
            StringBuilder conditionClauses = new StringBuilder();
            conditionClauses.append("( ").append(split.getLowerClause());
            conditionClauses.append(" ) AND ( ").append(split.getUpperClause());
            conditionClauses.append(" )");

            query.append("SELECT ");

            for (int i = 0; i < fieldNames.length; i++) {
                query.append(fieldNames[i]);

                if (i != fieldNames.length - 1)
                    query.append(", ");
            }

            query.append(" FROM ").append(tableName);

            if (dbConf.getTableAlias()) {
                query.append(" AS ").append(tableName); //in hsqldb this is necessary
            }

            query.append(" WHERE ");
            if (conditions != null && conditions.length() > 0) {
                // Put the user's conditions first.
                query.append("( ").append(conditions).append(" ) AND ");
            }

            // Now append the conditions associated with our split.
            query.append(conditionClauses.toString());

            String orderBy = dbConf.getInputOrderBy();

            if (orderBy != null && orderBy.length() > 0)
                query.append(" ORDER BY ").append(orderBy);

            return query.toString();
        }

        /** {@inheritDoc} */
        public void close() throws IOException {
            try {
                connection.commit();
                results.close();
                statement.close();
            } catch (SQLException exception) {
                throw new IOException("unable to commit and close", exception);
            }
        }

        /** {@inheritDoc} */
        public LongWritable createKey() {
            return new LongWritable();
        }

        /** {@inheritDoc} */
        public T createValue() {
            return ReflectionUtils.newInstance(inputClass, job);
        }

        /** {@inheritDoc} */
        public long getPos() throws IOException {
            return pos;
        }

        /** {@inheritDoc} */
        public float getProgress() throws IOException {
            return isDone()? 1.0f : 0.0f;
        }

        /**
         * @return true if nextKeyValue() would return false.
         */
        protected boolean isDone() {
            try {
                return this.results != null && results.isAfterLast();
            } catch (SQLException sqlE) {
                return true;
            }
        }

        /** {@inheritDoc} */
        public boolean next(LongWritable key, T value) throws IOException {
            try {
                if (!results.next())
                    return false;

                // Set the key field value as the output key value
                key.set(pos + split.getStart());

                value.readFields(results);

                pos++;
            } catch (SQLException exception) {
                throw new IOException("unable to get next value", exception);
            }

            return true;
        }
    }

    /**
     * A InputSplit that spans a set of rows
     */
    public static class DataDrivenDBInputSplit extends DBInputSplit {

        private String lowerBoundClause;
        private String upperBoundClause;

        /**
         * Default Constructor
         */
        public DataDrivenDBInputSplit() {
        }

        /**
         * Convenience Constructor
         * @param lower the string to be put in the WHERE clause to guard on the 'lower' end
         * @param upper the string to be put in the WHERE clause to guard on the 'upper' end
         */
        public DataDrivenDBInputSplit(final String lower, final String upper) {
            this.lowerBoundClause = lower;
            this.upperBoundClause = upper;
        }

        /**
         * @return The total row count in this split
         */
        public long getLength() throws IOException {
            return 0; // unfortunately, we don't know this.
        }

        /** {@inheritDoc} */
        public void readFields(DataInput input) throws IOException {
            this.lowerBoundClause = Text.readString(input);
            this.upperBoundClause = Text.readString(input);
        }

        /** {@inheritDoc} */
        public void write(DataOutput output) throws IOException {
            Text.writeString(output, this.lowerBoundClause);
            Text.writeString(output, this.upperBoundClause);
        }

        public String getLowerClause() {
            return lowerBoundClause;
        }

        public String getUpperClause() {
            return upperBoundClause;
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    public RecordReader<LongWritable, T> getRecordReader(InputSplit split, JobConf job,
                                                         Reporter reporter) throws IOException {
        Class inputClass = dbConf.getInputClass();
        try {
            return new DataDrivenDBRecordReader((DataDrivenDBInputSplit) split, inputClass, job);
        } catch (SQLException exception) {
            throw new IOException(exception.getMessage(), exception);
        }
    }

    /**
     * @return the DBSplitter implementation to use to divide the table/query into InputSplits.
     */
    protected DBSplitter getSplitter(int sqlDataType) {
        switch (sqlDataType) {
            case Types.NUMERIC:
            case Types.DECIMAL:
                return new BigDecimalSplitter();

            case Types.BIT:
            case Types.BOOLEAN:
                return new BooleanSplitter();

            case Types.INTEGER:
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.BIGINT:
                return new IntegerSplitter();

            case Types.REAL:
            case Types.FLOAT:
            case Types.DOUBLE:
                return new FloatSplitter();

            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return new TextSplitter();

            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                return new DateSplitter();

            default:
                // TODO: Support BINARY, VARBINARY, LONGVARBINARY, DISTINCT, CLOB, BLOB, ARRAY
                // STRUCT, REF, DATALINK, and JAVA_OBJECT.
                return null;
        }
    }

    public InputSplit[] getSplits(JobConf job, int chunks) throws IOException {
        int targetNumTasks = dbConf.getMaxConcurrentReadsNum();
        if (1 == targetNumTasks) {
            // There's no need to run a bounding vals query; just return a split
            // that separates nothing. This can be considerably more optimal for a
            // large table with no index.
            List<InputSplit> singletonSplit = new ArrayList<InputSplit>();
            singletonSplit.add(new DataDrivenDBInputSplit("1=1", "1=1"));
            return singletonSplit.toArray(new InputSplit[1]);
        }

        ResultSet results = null;
        Statement statement = null;
        try {
            statement = connection.createStatement();

            String boundingValsQuery = getBoundingValsQuery();

            LOG.info("Getting splits: " + boundingValsQuery);
            results = statement.executeQuery(boundingValsQuery);
            results.next();

            // Based on the type of the results, use a different mechanism
            // for interpolating split points (i.e., numeric splits, text splits,
            // dates, etc.)
            int sqlDataType = results.getMetaData().getColumnType(1);
            DBSplitter splitter = getSplitter(sqlDataType);
            if (null == splitter) {
                throw new IOException("Unknown SQL data type: " + sqlDataType);
            }
            List<InputSplit> splits = splitter.split(job, results, dbConf.getInputOrderBy());
            return splits.toArray(new InputSplit[splits.size()]);
        } catch (SQLException e) {
            throw new IOException(e.getMessage());
        } finally {
            // More-or-less ignore SQL exceptions here, but log in case we need it.
            try {
                if (null != results) {
                    results.close();
                }
            } catch (SQLException se) {
                LOG.debug("SQLException closing resultset: " + se.toString());
            }

            try {
                if (null != statement) {
                    statement.close();
                }
            } catch (SQLException se) {
                LOG.debug("SQLException closing statement: " + se.toString());
            }
        }
    }

    /**
     * @return a query which returns the minimum and maximum values for
     * the order-by column.
     *
     * The min value should be in the first column, and the
     * max value should be in the second column of the results.
     */
    protected String getBoundingValsQuery() {
        // Auto-generate one based on the table name we've been provided with.
        StringBuilder query = new StringBuilder();

        String splitCol = dbConf.getInputOrderBy();
        query.append("SELECT MIN(").append(splitCol).append("), ");
        query.append("MAX(").append(splitCol).append(") FROM ");
        query.append(dbConf.getInputTableName());
        String conditions = dbConf.getInputConditions();
        if (null != conditions) {
            query.append(" WHERE ( " + conditions + " )");
        }

        return query.toString();
    }

    /** {@inheritDoc} */
    public static void setInput(JobConf job, Class<? extends DBWritable> inputClass,
                                String tableName, String conditions, String orderBy, long limit, int concurrentReads,
                                Boolean tableAlias, String... fieldNames) {
        DBInputFormat.setInput(job, inputClass, tableName, conditions, orderBy, limit, concurrentReads, tableAlias, fieldNames);
        job.setInputFormat(DataDrivenDBInputFormat.class);
    }
}
