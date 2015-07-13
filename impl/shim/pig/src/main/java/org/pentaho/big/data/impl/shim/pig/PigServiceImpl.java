package org.pentaho.big.data.impl.shim.pig;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.WriterAppender;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.bigdata.api.pig.PigResult;
import org.pentaho.bigdata.api.pig.PigService;
import org.pentaho.bigdata.api.pig.impl.PigResultImpl;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.logging.KettleLogChannelAppender;
import org.pentaho.di.core.logging.Log4jFileAppender;
import org.pentaho.di.core.logging.Log4jKettleLayout;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.logging.LogWriter;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.hadoop.shim.spi.PigShim;

import java.io.File;
import java.io.PrintWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by bryan on 7/6/15.
 */
public class PigServiceImpl implements PigService {
  private static final Class<?> PKG = PigServiceImpl.class;
  private final NamedCluster namedCluster;
  private final PigShim pigShim;
  private final HadoopShim hadoopShim;

  public PigServiceImpl( NamedCluster namedCluster, PigShim pigShim, HadoopShim hadoopShim ) {
    this.namedCluster = namedCluster;
    this.pigShim = pigShim;
    this.hadoopShim = hadoopShim;
  }

  @Override public boolean isLocalExecutionSupported() {
    return pigShim.isLocalExecutionSupported();
  }

  @Override
  public PigResult executeScript( String scriptPath, ExecutionMode executionMode, List<String> parameters, String name,
                                  LogChannelInterface logChannelInterface, VariableSpace variableSpace,
                                  LogLevel logLevel ) {
    // Set up an appender that will send all pig log messages to Kettle's log
    // via logBasic().
    KettleLoggingPrintWriter klps = new KettleLoggingPrintWriter( logChannelInterface );
    WriterAppender pigToKettleAppender = new WriterAppender( new Log4jKettleLayout( true ), klps );

    Logger pigLogger = Logger.getLogger( "org.apache.pig" );
    Level log4jLevel = getLog4jLevel( logLevel );
    pigLogger.setLevel( log4jLevel );
    Log4jFileAppender appender = null;
    String logFileName = "pdi-" + name; //$NON-NLS-1$
    LogWriter logWriter = LogWriter.getInstance();
    try {
      appender = LogWriter.createFileAppender( logFileName, true, false );
      logWriter.addAppender( appender );
      logChannelInterface.setLogLevel( logLevel );
      if ( pigLogger != null ) {
        pigLogger.addAppender( pigToKettleAppender );
      }
    } catch ( Exception e ) {
      logChannelInterface.logError( BaseMessages
        .getString( PKG, "JobEntryPigScriptExecutor.FailedToOpenLogFile", logFileName, e.toString() ) ); //$NON-NLS-1$
      logChannelInterface.logError( Const.getStackTracker( e ) );
    }

    try {
      Configuration configuration = hadoopShim.createConfiguration();
      if ( executionMode != ExecutionMode.LOCAL ) {
        List<String> configMessages = new ArrayList<String>();
        hadoopShim.configureConnectionInformation( variableSpace.environmentSubstitute( namedCluster.getHdfsHost() ),
          variableSpace.environmentSubstitute( namedCluster.getHdfsPort() ),
          variableSpace.environmentSubstitute( namedCluster.getJobTrackerHost() ),
          variableSpace.environmentSubstitute( namedCluster.getJobTrackerPort() ), configuration,
          configMessages );
        if ( logChannelInterface != null ) {
          for ( String configMessage : configMessages ) {
            logChannelInterface.logBasic( configMessage );
          }
        }
      }
      URL scriptU;
      String scriptFileS = scriptPath;
      scriptFileS = variableSpace.environmentSubstitute( scriptFileS );
      if ( scriptFileS.indexOf( "://" ) == -1 ) {
        File scriptFile = new File( scriptFileS );
        scriptU = scriptFile.toURI().toURL();
      } else {
        scriptU = new URL( scriptFileS );
      }
      String pigScript = pigShim.substituteParameters( scriptU, parameters );
      Properties properties = new Properties();
      pigShim.configure( properties, executionMode == ExecutionMode.LOCAL ? null : configuration );
      return new PigResultImpl( appender == null ? null : appender.getFile(),
        pigShim.executeScript( pigScript, executionMode == ExecutionMode.LOCAL ? PigShim.ExecutionMode.LOCAL :
          PigShim.ExecutionMode.MAPREDUCE, properties ), null );
    } catch ( Exception e ) {
      return new PigResultImpl( appender == null ? null : appender.getFile(), null, e );
    } finally {
      removeAppender( appender, pigToKettleAppender );
    }
  }

  private Level getLog4jLevel( LogLevel level ) {
    // KettleLogChannelAppender does not exists in Kette core, so we'll use it from kettle5-log4j-plugin.
    Level log4jLevel = KettleLogChannelAppender.LOG_LEVEL_MAP.get( level );
    return log4jLevel != null ? log4jLevel : Level.INFO;
  }

  protected void removeAppender( Log4jFileAppender appender, WriterAppender pigToKettleAppender ) {

    // remove the file appender from kettle logging
    if ( appender != null ) {
      LogWriter.getInstance().removeAppender( appender );
      appender.close();
    }

    Logger pigLogger = Logger.getLogger( "org.apache.pig" );
    if ( pigLogger != null && pigToKettleAppender != null ) {
      pigLogger.removeAppender( pigToKettleAppender );
      pigToKettleAppender.close();
    }
  }

  /**
   * An extended PrintWriter that sends output to Kettle's logging
   *
   * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
   */
  class KettleLoggingPrintWriter extends PrintWriter {
    private final LogChannelInterface logChannelInterface;

    public KettleLoggingPrintWriter( LogChannelInterface logChannelInterface ) {
      super( System.out );
      this.logChannelInterface = logChannelInterface;
    }

    @Override
    public void println( String string ) {
      logChannelInterface.logBasic( string );
    }

    @Override
    public void println( Object obj ) {
      println( obj.toString() );
    }

    @Override
    public void write( String string ) {
      println( string );
    }

    @Override
    public void print( String string ) {
      println( string );
    }

    @Override
    public void print( Object obj ) {
      print( obj.toString() );
    }

    @Override
    public void close() {
      flush();
    }
  }
}
