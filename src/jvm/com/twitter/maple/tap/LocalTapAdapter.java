package com.twitter.maple.tap;

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
 * This class is intended to wrap a Hadoop-based Tap to be a Local-tap.
 */
public class LocalTapAdapter<HADOOP_TAP extends Tap, SourceCtx, SinkCtx> extends Tap<Properties, RecordReader, OutputCollector> {
  private static final long serialVersionUID = 8552212538252771309L;

  private static Logger LOG = Logger.getLogger( LocalTapAdapter.class.getName() );

  private JobConf defaults;

  private HADOOP_TAP tap;

  private final String path;


  /**
   * 
   * @param scheme Scheme to wrap.
   * @param sinkMode Sink Mode to use
   * @param tap Tap to wrap
   * @param path Path for this tap
   * @param config Default configuration required for the tap.
   */
  public LocalTapAdapter( Scheme scheme, SinkMode sinkMode, HADOOP_TAP tap, String path, JobConf config ) {
    super( new LocalScheme<SourceCtx, SinkCtx>( scheme ), sinkMode );
    this.tap = tap;
    this.path = path;
    setup( config );
  }


  private void setup( JobConf config ) {
    /*
     * LocalTapAdapter requires your system Hadoop configuration for
     * defaults to supply the wrapped tap. Make sure you have your
     * serializations and serialization tokens defined there.
     */
    defaults = new JobConf( config );

    // HACK: c.t.h.TextLine checks this property for .zip files; the check
    // assumes the list is non-empty, which we mock up, here
    defaults.setStrings( "io.serializations", new String[] { "org.apache.hadoop.io.serializer.WritableSerialization", "cascading.tuple.hadoop.TupleSerialization" } );

    ( (LocalScheme<SourceCtx, SinkCtx>) this.getScheme() ).setDefaults( defaults );

    ( (LocalScheme<SourceCtx, SinkCtx>) this.getScheme() ).setTap( tap );
  }


  @Override
  public String getIdentifier() {
    return path;
  }


  @Override
  public TupleEntryIterator openForRead( FlowProcess<Properties> flowProcess, RecordReader input ) throws IOException {
    JobConf jobConf = mergeDefaults( "LocalTapAdapter#openForRead", flowProcess.getConfigCopy(), defaults );
    return tap.openForRead( new HadoopFlowProcess( jobConf ) );
  }


  @Override
  public TupleEntryCollector openForWrite( FlowProcess<Properties> flowProcess, OutputCollector output ) throws IOException {
    JobConf jobConf = mergeDefaults( "LocalTapAdapter#openForWrite", flowProcess.getConfigCopy(), defaults );
    return tap.openForWrite( new HadoopFlowProcess( jobConf ) );
  }


  @Override
  public boolean createResource( Properties conf ) throws IOException {
    return tap.createResource( mergeDefaults( "LocalTapAdapter#createResource", conf, defaults ) );
  }


  @Override
  public boolean deleteResource( Properties conf ) throws IOException {
    return tap.deleteResource( mergeDefaults( "LocalTapAdapter#deleteResource", conf, defaults ) );
  }


  @Override
  public boolean resourceExists( Properties conf ) throws IOException {
    return tap.resourceExists( mergeDefaults( "LocalTapAdapter#resourceExists", conf, defaults ) );
  }


  @Override
  public long getModifiedTime( Properties conf ) throws IOException {
    return tap.getModifiedTime( mergeDefaults( "LocalTapAdapter#getModifiedTime", conf, defaults ) );
  }


  private static JobConf mergeDefaults( String methodName, Properties properties, JobConf defaults ) {
    LOG.fine( methodName + " is merging defaults with: " + properties );
    return HadoopUtil.createJobConf( properties, defaults );
  }


  private static Properties overwriteProperties( Properties properties, JobConf jobConf ) {
    for ( Map.Entry<String, String> entry : jobConf ) {
      properties.setProperty( entry.getKey(), entry.getValue() );
    }
    return properties;
  }

  /**
   * Scheme used only for fields and configuration.
   */
  private static class LocalScheme<SourceContext, SinkContext> extends Scheme<Properties, RecordReader, OutputCollector, SourceContext, SinkContext> {
    private static final long serialVersionUID = 5710119342340369543L;

    private Scheme<JobConf, RecordReader, OutputCollector, SourceContext, SinkContext> scheme;

    private JobConf defaults;

    private Tap tap;


    public LocalScheme( Scheme<JobConf, RecordReader, OutputCollector, SourceContext, SinkContext> scheme ) {
      super( scheme.getSourceFields(), scheme.getSinkFields() );
      this.scheme = scheme;
    }


    private void setDefaults( JobConf defaults ) {
      this.defaults = defaults;
    }


    private void setTap( Tap tap ) {
      this.tap = tap;
    }


    @Override
    public Fields retrieveSourceFields( FlowProcess<Properties> flowProcess, Tap tap ) {
      return scheme.retrieveSourceFields( new HadoopFlowProcess( defaults ), this.tap );
    }


    @Override
    public void presentSourceFields( FlowProcess<Properties> flowProcess, Tap tap, Fields fields ) {
      scheme.presentSourceFields( new HadoopFlowProcess( defaults ), this.tap, fields );
    }


    @Override
    public void sourceConfInit( FlowProcess<Properties> flowProcess, Tap<Properties, RecordReader, OutputCollector> tap, Properties conf ) {
      JobConf jobConf = mergeDefaults( "LocalScheme#sourceConfInit", conf, defaults );
      scheme.sourceConfInit( new HadoopFlowProcess( jobConf ), this.tap, jobConf );
      overwriteProperties( conf, jobConf );
    }


    @Override
    public void sinkConfInit( FlowProcess<Properties> flowProcess, Tap<Properties, RecordReader, OutputCollector> tap, Properties conf ) {
      JobConf jobConf = mergeDefaults( "LocalScheme#sinkConfInit", conf, defaults );
      scheme.sinkConfInit( new HadoopFlowProcess( jobConf ), this.tap, jobConf );
      overwriteProperties( conf, jobConf );
    }


    @Override
    public Fields retrieveSinkFields( FlowProcess<Properties> flowProcess, Tap tap ) {
      return scheme.retrieveSinkFields( new HadoopFlowProcess( defaults ), this.tap );
    }


    @Override
    public void presentSinkFields( FlowProcess<Properties> flowProcess, Tap tap, Fields fields ) {
      scheme.presentSinkFields( new HadoopFlowProcess( defaults ), this.tap, fields );
    }


    @Override
    public boolean source( FlowProcess<Properties> flowProcess, SourceCall<SourceContext, RecordReader> sourceCall ) throws IOException {
      throw new RuntimeException( "LocalTapAdapter#source is never called" );
    }


    @Override
    public void sink( FlowProcess<Properties> flowProcess, SinkCall<SinkContext, OutputCollector> sinkCall ) throws IOException {
      throw new RuntimeException( "LocalTapAdapter#sink is never called" );
    }
  }


  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ( ( defaults == null ) ? 0 : defaults.hashCode() );
    result = prime * result + ( ( path == null ) ? 0 : path.hashCode() );
    result = prime * result + ( ( tap == null ) ? 0 : tap.hashCode() );
    return result;
  }


  @Override
  public boolean equals( Object obj ) {
    if ( this == obj )
      return true;
    if ( !super.equals( obj ) )
      return false;
    if ( getClass() != obj.getClass() )
      return false;
    LocalTapAdapter other = (LocalTapAdapter) obj;
    if ( defaults == null ) {
      if ( other.defaults != null )
        return false;
    } else if ( !defaults.equals( other.defaults ) )
      return false;
    if ( path == null ) {
      if ( other.path != null )
        return false;
    } else if ( !path.equals( other.path ) )
      return false;
    if ( tap == null ) {
      if ( other.tap != null )
        return false;
    } else if ( !tap.equals( other.tap ) )
      return false;
    return true;
  }


  /** @see Object#toString() */
  @Override
  public String toString() {
    if ( path != null ) {
      return getClass().getSimpleName() + "[\"" + getScheme() + "\"]" + "[\"" + Util.sanitizeUrl( path ) + "\"]";
    } else {
      return getClass().getSimpleName() + "[\"" + getScheme() + "\"]" + "[not initialized]";
    }
  }
}
