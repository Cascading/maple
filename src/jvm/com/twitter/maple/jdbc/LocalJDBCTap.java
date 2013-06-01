package com.twitter.maple.jdbc;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.util.Util;

/**
 * A shunt around the restrictions on taps that can run under the
 * LocalFlowProcess. This class delegates to Lfs for all work, yet can run in
 * Cascading local mode.
 *
 * Due to what appears to be a bug in LocalFlowStep#initTaps, schemes are not
 * allowed to side-effect the properties passed to sourceConfInit and
 * sinkConfInit. This appears to be a non-issue because local schemes never set
 * any properties. overwriteProperties can likely be removed, but is kept in
 * case custom local schemes eventually set properties (in a future release of
 * Cascading where that's possible).
 * 
 * This is effectively a copy of the com.etsy.cascading.tap.local.LocalTap, but
 * modified for the JDBCTap-ish.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class LocalJDBCTap<SourceCtx, SinkCtx> extends Tap<Properties, RecordReader, OutputCollector> {
    private static final long serialVersionUID = 3480480638297770870L;

    private static Logger LOG = Logger.getLogger(LocalJDBCTap.class.getName());

    private JobConf defaults;
    private JDBCTap tap;
    private final String path;

    public LocalJDBCTap(Scheme scheme,
            SinkMode sinkMode, JDBCTap tap, String path) {
        super(new LocalScheme<SourceCtx, SinkCtx>(scheme), sinkMode);
        this.tap = tap;
        this.path = path;
        setup();
    }

    private void setup() {
        /*
         * LocalJDBCTap requires your system Hadoop configuration for defaults to
         * supply the wrapped Lfs. Make sure you have your serializations and
         * serialization tokens defined there.
         */
        defaults = new JobConf();

        ((LocalScheme<SourceCtx, SinkCtx>) this.getScheme()).setDefaults(defaults);

        ((LocalScheme<SourceCtx, SinkCtx>) this.getScheme()).setTap(tap);
    }

    @Override
    public String getIdentifier() {
        return path;
    }

    @Override
    public TupleEntryIterator openForRead(FlowProcess<Properties> flowProcess, RecordReader input) throws IOException {
        JobConf jobConf = mergeDefaults("LocalJDBCTap#openForRead", flowProcess.getConfigCopy(), defaults);
        return tap.openForRead(new HadoopFlowProcess(jobConf));
    }

    @Override
    public TupleEntryCollector openForWrite(FlowProcess<Properties> flowProcess, OutputCollector output)
            throws IOException {
        JobConf jobConf = mergeDefaults("LocalJDBCTap#openForWrite", flowProcess.getConfigCopy(), defaults);
        return tap.openForWrite(new HadoopFlowProcess(jobConf));
    }

    @Override
    public boolean createResource(Properties conf) throws IOException {
        return tap.createResource(mergeDefaults("LocalJDBCTap#createResource", conf, defaults));
    }

    @Override
    public boolean deleteResource(Properties conf) throws IOException {
        return tap.deleteResource(mergeDefaults("LocalJDBCTap#deleteResource", conf, defaults));
    }

    @Override
    public boolean resourceExists(Properties conf) throws IOException {
        return tap.resourceExists(mergeDefaults("LocalJDBCTap#resourceExists", conf, defaults));
    }

    @Override
    public long getModifiedTime(Properties conf) throws IOException {
        return tap.getModifiedTime(mergeDefaults("LocalJDBCTap#getModifiedTime", conf, defaults));
    }

    private static JobConf mergeDefaults(String methodName, Properties properties, JobConf defaults) {
        LOG.fine(methodName + " is merging defaults with: " + properties);
        return HadoopUtil.createJobConf(properties, defaults);
    }

    private static Properties overwriteProperties(Properties properties, JobConf jobConf) {
        for (Map.Entry<String, String> entry : jobConf) {
            properties.setProperty(entry.getKey(), entry.getValue());
        }
        return properties;
    }

    /**
     * Scheme used only for fields and configuration.
     */
    private static class LocalScheme<SourceContext, SinkContext> extends
            Scheme<Properties, RecordReader, OutputCollector, SourceContext, SinkContext> {
        private static final long serialVersionUID = 5710119342340369543L;

        private Scheme<JobConf, RecordReader, OutputCollector, SourceContext, SinkContext> scheme;
        private JobConf defaults;
        private Tap tap;

        public LocalScheme(Scheme<JobConf, RecordReader, OutputCollector, SourceContext, SinkContext> scheme) {
            super(scheme.getSourceFields(), scheme.getSinkFields());
            this.scheme = scheme;
        }

        private void setDefaults(JobConf defaults) {
            this.defaults = defaults;
        }

        private void setTap(Tap tap) {
            this.tap = tap;
        }

        @Override
        public void sourceConfInit(FlowProcess<Properties> flowProcess,
                Tap<Properties, RecordReader, OutputCollector> tap, Properties conf) {
            JobConf jobConf = mergeDefaults("LocalScheme#sourceConfInit", conf, defaults);
            scheme.sourceConfInit(new HadoopFlowProcess(jobConf), this.tap, jobConf);
            overwriteProperties(conf, jobConf);
        }

        @Override
        public void sinkConfInit(FlowProcess<Properties> flowProcess,
                Tap<Properties, RecordReader, OutputCollector> tap, Properties conf) {
            JobConf jobConf = mergeDefaults("LocalScheme#sinkConfInit", conf, defaults);
            scheme.sinkConfInit(new HadoopFlowProcess(jobConf), this.tap, jobConf);
            overwriteProperties(conf, jobConf);
        }

        @Override
        public boolean source(FlowProcess<Properties> flowProcess, SourceCall<SourceContext, RecordReader> sourceCall)
                throws IOException {
            throw new RuntimeException("LocalJDBCTap#source is never called");
        }

        @Override
        public void sink(FlowProcess<Properties> flowProcess, SinkCall<SinkContext, OutputCollector> sinkCall)
                throws IOException {
            throw new RuntimeException("LocalJDBCTap#sink is never called");
        }

        @Override
        public Fields getSinkFields() {
            if ( scheme != null ) {
                return scheme.getSinkFields();
            } else {
                return super.getSinkFields();
            }
        }

        @Override
        public void setSinkFields( Fields sinkFields ) {
          if ( scheme != null ) {
              scheme.setSinkFields( sinkFields );
              super.setSinkFields( getSinkFields() );
          } else {
              super.setSinkFields( sinkFields );
          }
        }

        @Override
        public Fields getSourceFields() {
            if ( scheme != null ) {
                return scheme.getSourceFields();
            } else {
                return super.getSourceFields();
            }
        }

        @Override
        public void setSourceFields( Fields sourceFields ) {
            if ( scheme != null ) {
                scheme.setSourceFields( sourceFields );
                super.setSourceFields( getSourceFields() );
            } else {
                super.setSourceFields( sourceFields );
            }
        }
    }

    @Override
    public boolean equals(Object object) {
        if (this == object)
            return true;
        if (!(object instanceof LocalJDBCTap))
            return false;
        if (!super.equals(object))
            return false;

        LocalJDBCTap localTap = (LocalJDBCTap) object;

        if (path != null ? !path.equals(localTap.path) : localTap.path != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (path != null ? path.hashCode() : 0);
        return result;
    }

    /** @see Object#toString() */
    @Override
    public String toString() {
        if (path != null)
            return getClass().getSimpleName() + "[\"" + getScheme() + "\"]" + "[\"" + Util.sanitizeUrl(path) + "\"]";
        else
            return getClass().getSimpleName() + "[\"" + getScheme() + "\"]" + "[not initialized]";
    }
}
