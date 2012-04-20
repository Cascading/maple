package com.twitter.maple.tap;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.hadoop.HadoopUtil;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class MemorySourceTap extends SourceTap<HadoopFlowProcess, JobConf, RecordReader> implements
    Serializable {
    private static final Logger logger = LoggerFactory.getLogger(MemorySourceTap.class);

    public static class MemorySourceScheme extends Scheme<HadoopFlowProcess, JobConf, RecordReader, Void, Object[], Void> {
        private static final Logger logger = LoggerFactory.getLogger(MemorySourceScheme.class);

        private transient List<Tuple> tuples;
        private final String id;

        public MemorySourceScheme(List<Tuple> tuples, Fields fields, String id) {
            super(fields);
            assert tuples != null;
            this.tuples = tuples;
            this.id = id;
        }

        public String getId() {
            return this.id;
        }

        public List<Tuple> getTuples()
        {
            return this.tuples;
        }

        @Override
        public void sourceConfInit(HadoopFlowProcess flowProcess, Tap tap, JobConf conf) {
            FileInputFormat.setInputPaths(conf, this.id);
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

    private final String id;
    private transient FileStatus[] statuses;

    public MemorySourceTap(List<Tuple> tuples, Fields fields) {
        super(new MemorySourceScheme(tuples, fields, "/" + UUID.randomUUID().toString()));
        this.id = ((MemorySourceScheme) this.getScheme()).getId();
    }

    @Override
    public String getIdentifier() {
        return getPath().toString();
    }

    public Path getPath() {
        return new Path(id);
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

    private JobConf getSourceConf( HadoopFlowProcess flowProcess, JobConf conf, String property ) throws IOException {
        Map<String, String> priorConf = HadoopUtil.deserializeMapBase64( property, true );
        return flowProcess.mergeMapIntoConfig( conf, priorConf );
    }

    @Override
    public TupleEntryIterator openForRead( HadoopFlowProcess flowProcess, RecordReader input ) throws IOException {
        String identifier = (String) flowProcess.getProperty( "cascading.source.path" );

        // this is only called cluster task side when Hadoop is providing a RecordReader instance it owns
        // during processing of an InputSplit
        if( input != null )
            return new TupleEntrySchemeIterator( flowProcess, getScheme(), new RecordReaderIterator( input ), identifier );

        Map<Object, Object> properties = HadoopUtil.createProperties(flowProcess.getJobConf());

        properties.remove( "mapred.input.dir" );

        JobConf conf = HadoopUtil.createJobConf( properties, null );

        // allows client side config to be used cluster side
        String property = flowProcess.getJobConf().getRaw( "cascading.step.accumulated.source.conf." + getIdentifier() );

        if( property != null ) {
            conf = getSourceConf( flowProcess, conf, property );
            flowProcess = new HadoopFlowProcess( flowProcess, conf );
        }

        // this is only called when, on the client side, a user wants to open a tap for writing on a client
        // MultiRecordReader will create a new RecordReader instance for use across any file parts
        // or on the cluster side during accumulation for a Join
        return new TupleEntrySchemeIterator( flowProcess, getScheme(),
            new MultiRecordReaderIterator( flowProcess, this, conf ), "MemoryTap: " + getIdentifier() );
    }

    @Override
    public long getModifiedTime( JobConf conf ) throws IOException {
        return System.currentTimeMillis();
    }
}
