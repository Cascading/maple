package com.twitter.maple.tap;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.SourceTap;
import cascading.tap.Tap;
import cascading.tap.hadoop.MultiRecordReaderIterator;
import cascading.tap.hadoop.RecordReaderIterator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntrySchemeIterator;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class MemorySourceTap extends SourceTap<HadoopFlowProcess, JobConf, RecordReader> {
    public static class MemorySourceScheme extends Scheme<HadoopFlowProcess, JobConf, RecordReader, Void, Object[], Void> {

        private transient List<Tuple> tuples;

        public MemorySourceScheme(List<Tuple> tuples, Fields fields) {
            super(fields);
            this.tuples = tuples;
        }

        public List<Tuple> getTuples()
        {
            return this.tuples;
        }

        @Override
        public void sourceConfInit(HadoopFlowProcess flowProcess, Tap tap, JobConf conf) {
            FileInputFormat.setInputPaths( conf, "/" + UUID.randomUUID().toString());
            conf.setInputFormat(TupleMemoryInputFormat.class);
            TupleMemoryInputFormat.storeTuples(conf, TupleMemoryInputFormat.TUPLES_PROPERTY, this.tuples);
        }

        @Override
        public void sinkConfInit(HadoopFlowProcess flowProcess, Tap tap, JobConf jc) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void sourcePrepare( HadoopFlowProcess flowProcess, SourceCall<Object[], RecordReader> sourceCall ) {
            sourceCall.setContext( new Object[ 2 ] );

            sourceCall.getContext()[ 0 ] = sourceCall.getInput().createKey();
            sourceCall.getContext()[ 1 ] = sourceCall.getInput().createValue();
        }

        @Override
        public boolean source(HadoopFlowProcess flowProcess, SourceCall<Object[], RecordReader> sourceCall) throws IOException {
            TupleWrapper key = (TupleWrapper) sourceCall.getContext()[ 0 ];
            NullWritable value = (NullWritable) sourceCall.getContext()[ 1 ];

            boolean result = sourceCall.getInput().next( key, value );

            if( !result )
                return false;

            sourceCall.getIncomingEntry().setTuple(key.tuple);
            return true;
        }

        @Override
        public void sourceCleanup( HadoopFlowProcess flowProcess, SourceCall<Object[], RecordReader> sourceCall ) {
            sourceCall.setContext( null );
        }

        @Override
        public void sink(HadoopFlowProcess flowProcess, SinkCall<Void, Void> sinkCall ) throws IOException {
            throw new UnsupportedOperationException("Not supported.");
        }

    }

    private String id = UUID.randomUUID().toString();
    private transient FileStatus[] statuses;

    public MemorySourceTap(List<Tuple> tuples, Fields fields) {
        super(new MemorySourceScheme(tuples, fields));
    }

    @Override
    public String getIdentifier() {
        return getPath().toString();
    }

    public Path getPath() {
        return new Path("/" + id);
    }

    @Override
    public boolean resourceExists( JobConf conf ) throws IOException {
        return true;
    }

    @Override
    public boolean equals(Object object) {
        if(!getClass().equals(object.getClass())) {
            return false;
        }
        MemorySourceTap other = (MemorySourceTap) object;
        return id.equals(other.id);
    }

    @Override
    public TupleEntryIterator openForRead( HadoopFlowProcess flowProcess, RecordReader input ) throws IOException {
        if (input != null)
            return new TupleEntrySchemeIterator( flowProcess, getScheme(), new RecordReaderIterator( input ) );

        JobConf conf = new JobConf( flowProcess.getJobConf() );

        return new TupleEntrySchemeIterator(flowProcess, getScheme(),
            new MultiRecordReaderIterator(flowProcess, this, conf), "MemoryTap: " + getIdentifier());
    }

    @Override
    public long getModifiedTime( JobConf conf ) throws IOException {
        return System.currentTimeMillis();
    }
}
