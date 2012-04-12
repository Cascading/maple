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

package com.twitter.maple.jdbc;

import cascading.tuple.Tuple;
import com.twitter.maple.jdbc.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TupleRecord implements DBWritable {
    private Tuple tuple;

    public TupleRecord() {
    }

    public TupleRecord( Tuple tuple ) {
        this.tuple = tuple;
    }

    public void setTuple( Tuple tuple ) {
        this.tuple = tuple;
    }

    public Tuple getTuple() {
        return tuple;
    }

    public void write( PreparedStatement statement ) throws SQLException {
        for( int i = 0; i < tuple.size(); i++ )
            statement.setObject( i + 1, tuple.get( i ) );
    }

    public void readFields( ResultSet resultSet ) throws SQLException {
        tuple = new Tuple();

        for( int i = 0; i < resultSet.getMetaData().getColumnCount(); i++ )
            tuple.add( (Comparable) resultSet.getObject( i + 1 ) );
    }

}
