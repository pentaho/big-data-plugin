/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.job.entries.sqoop;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.database.DatabaseInterface;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.AbstractJobEntry;
import org.pentaho.di.job.JobEntryUtils;
import org.pentaho.di.job.LoggingProxy;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.hadoop.shim.spi.SqoopShim;
import org.w3c.dom.Node;

/**
 * Base class for all Sqoop job entries.
 */
public abstract class AbstractSqoopJobEntry<S extends SqoopConfig> extends AbstractJobEntry<S> implements Cloneable,
    JobEntryInterface {

  /**
   * Log4j appender that redirects all Log4j logging to a Kettle {@link org.pentaho.di.core.logging.LogChannel}
   */
  private Appender sqoopToKettleAppender;

  /**
   * Logging proxy that redirects all {@link java.io.PrintStream} output to a Log4j logger.
   */
  private LoggingProxy stdErrProxy;

  /**
   * Logging categories to monitor and log within Kettle
   */
  private String[] LOGS_TO_MONITOR = new String[] { "org.apache.sqoop", "org.apache.hadoop" };

  /**
   * Cache for the levels of loggers we changed so we can revert them when we remove our appender
   */
  private Map<String, Level> logLevelCache = new HashMap<String, Level>();

  /**
   * Build a configuration object that contains all configuration settings for this job entry. This will be configured
   * by {@link #createJobConfig} and is not intended to be used directly.
   * 
   * @return a {@link SqoopConfig} object that contains all configuration settings for this job entry
   */
  protected abstract S buildSqoopConfig();

  /**
   * Declare the {@link Sqoop} tool used in this job entry.
   * 
   * @return the name of the sqoop tool to use, e.g. "import"
   */
  protected abstract String getToolName();

  public AbstractSqoopJobEntry() {
    super();
  }

  protected AbstractSqoopJobEntry( LogChannelInterface logChannelInterface ) {
    super( logChannelInterface );
  }

  /**
   * @return a {@link SqoopConfig} that contains all configuration settings for this job entry
   */
  @Override
  protected final S createJobConfig() {
    S config = buildSqoopConfig();
    try {
      HadoopShim shim =
          HadoopConfigurationBootstrap.getHadoopConfigurationProvider().getActiveConfiguration().getHadoopShim();
      Configuration hadoopConfig = shim.createConfiguration();
      SqoopUtils.configureConnectionInformation( config, shim, hadoopConfig );
    } catch ( Exception ex ) {
      // Error loading connection information from Hadoop Configuration. Just log the error and leave the configuration
      // as is.
      logError( BaseMessages.getString( AbstractSqoopJobEntry.class, "ErrorLoadingHadoopConnectionInformation" ), ex );
    }
    return config;
  }

  @Override
  public void loadXML( Node node, List<DatabaseMeta> databaseMetas, List<SlaveServer> slaveServers,
      Repository repository ) throws KettleXMLException {
    super.loadXML( node, databaseMetas, slaveServers, repository );
    // sync up the advanced configuration if no database type is set
    if ( getJobConfig().getDatabase() == null ) {
      getJobConfig().copyConnectionInfoToAdvanced();
    }
  }

  @Override
  public void loadRep( Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases,
      List<SlaveServer> slaveServers ) throws KettleException {
    super.loadRep( rep, id_jobentry, databases, slaveServers );
    // sync up the advanced configuration if no database type is set
    if ( getJobConfig().getDatabase() == null ) {
      getJobConfig().copyConnectionInfoToAdvanced();
    }
  }

  /**
   * Attach a log appender to all Loggers used by Sqoop so we can redirect the output to Kettle's logging facilities.
   */
  @SuppressWarnings( "deprecation" )
  public void attachLoggingAppenders() {
    sqoopToKettleAppender = new org.pentaho.di.core.logging.KettleLogChannelAppender( log );
    try {
      // Redirect all stderr logging to the first log to monitor so it shows up in the Kettle LogChannel
      Logger sqoopLogger = JobEntryUtils.findLogger( LOGS_TO_MONITOR[0] );
      if ( sqoopLogger != null ) {
        stdErrProxy = new LoggingProxy( System.err, sqoopLogger, Level.ERROR );
        System.setErr( stdErrProxy );
      }
      JobEntryUtils.attachAppenderTo( sqoopToKettleAppender, getLogLevel(), logLevelCache, LOGS_TO_MONITOR );
    } catch ( Exception ex ) {
      logMinimal( BaseMessages.getString( AbstractSqoopJobEntry.class, "ErrorAttachingLogging" ) );
      logDebug( Const.getStackTracker( ex ) );

      // Attempt to clean up logging if we failed
      try {
        JobEntryUtils.removeAppenderFrom( sqoopToKettleAppender, logLevelCache, LOGS_TO_MONITOR );
      } catch ( Exception e ) {
        // Ignore any exceptions while trying to clean up
      }
      sqoopToKettleAppender = null;
    }
  }

  /**
   * Remove our log appender from all loggers used by Sqoop.
   */
  public void removeLoggingAppenders() {
    try {
      if ( sqoopToKettleAppender != null ) {
        JobEntryUtils.removeAppenderFrom( sqoopToKettleAppender, logLevelCache, LOGS_TO_MONITOR );
        sqoopToKettleAppender = null;
      }
      if ( stdErrProxy != null ) {
        System.setErr( stdErrProxy.getWrappedStream() );
        stdErrProxy = null;
      }
    } catch ( Exception ex ) {
      logError( BaseMessages.getString( AbstractSqoopJobEntry.class, "ErrorDetachingLogging" ) );
      logError( Const.getStackTracker( ex ) );
    }
  }

  /**
   * Validate any configuration option we use directly that could be invalid at runtime.
   * 
   * @param config
   *          Configuration to validate
   * @return List of warning messages for any invalid configuration options we use directly in this job entry.
   */
  @Override
  public List<String> getValidationWarnings( SqoopConfig config ) {
    List<String> warnings = new ArrayList<String>();

    if ( StringUtil.isEmpty( config.getConnect() ) ) {
      warnings.add( BaseMessages.getString( AbstractSqoopJobEntry.class, "ValidationError.Connect.Message", config
          .getConnect() ) );
    }

    try {
      JobEntryUtils.asLong( config.getBlockingPollingInterval(), variables );
    } catch ( NumberFormatException ex ) {
      warnings.add( BaseMessages.getString( AbstractSqoopJobEntry.class,
          "ValidationError.BlockingPollingInterval.Message", config.getBlockingPollingInterval() ) );
    }

    return warnings;
  }

  /**
   * Handle any clean up required when our execution thread encounters an unexpected {@link Exception}.
   * 
   * @param t
   *          Thread that encountered the uncaught exception
   * @param e
   *          Exception that was encountered
   * @param jobResult
   *          Job result for the execution that spawned the thread
   */
  @Override
  protected void handleUncaughtThreadException( Thread t, Throwable e, Result jobResult ) {
    logError( BaseMessages.getString( AbstractSqoopJobEntry.class, "ErrorRunningSqoopTool" ), e );
    removeLoggingAppenders();
    setJobResultFailed( jobResult );
  }

  /**
   * @param shim
   *          Hadoop shim to load configuration from
   * @return the Hadoop configuration object for this Sqoop execution
   */
  protected Configuration getHadoopConfiguration( HadoopShim shim ) {
    return shim.createConfiguration();
  }

  @Override
  protected Runnable getExecutionRunnable( final Result jobResult ) throws KettleException {
    try {
      HadoopConfiguration activeConfig =
          HadoopConfigurationBootstrap.getHadoopConfigurationProvider().getActiveConfiguration();
      final HadoopShim hadoopShim = activeConfig.getHadoopShim();
      final SqoopShim sqoopShim = activeConfig.getSqoopShim();

      Runnable runnable = new Runnable() {
        @Override
        public void run() {
          executeSqoop( hadoopShim, sqoopShim, getJobConfig(), getHadoopConfiguration( hadoopShim ), jobResult );
        }
      };
      return runnable;
    } catch ( ConfigurationException ex ) {
      throw new KettleException( ex );
    }
  }

  /**
   * Executes Sqoop using the provided configuration objects. The {@code jobResult} will accurately reflect the
   * completed execution state when finished.
   * 
   * @param hadoopShim
   *          Hadoop Shim to use
   * @param sqoopShim
   *          Sqoop Shim to use
   * @param config
   *          Sqoop configuration settings
   * @param hadoopConfig
   *          Hadoop configuration settings. This will be additionally configured using
   *          {@link #configure(org.apache.hadoop.conf.Configuration)}.
   * @param jobResult
   *          Result to update based on feedback from the Sqoop tool
   */
  protected void executeSqoop( HadoopShim hadoopShim, SqoopShim shim, S config, Configuration hadoopConfig,
      Result jobResult ) {
    // Make sure Sqoop throws exceptions instead of returning a status of 1
    System.setProperty( "sqoop.throwOnError", "true" );

    attachLoggingAppenders();
    try {
      configure( hadoopShim, config, hadoopConfig );
      List<String> args = SqoopUtils.getCommandLineArgs( config, getVariables() );
      args.add( 0, getToolName() ); // push the tool command-line argument on the top of the args list
      int result = shim.runTool( args.toArray( new String[args.size()] ), hadoopConfig );
      if ( result != 0 ) {
        setJobResultFailed( jobResult );
      }
    } catch ( Exception ex ) {
      logError( BaseMessages.getString( AbstractSqoopJobEntry.class, "ErrorRunningSqoopTool" ), ex );
      setJobResultFailed( jobResult );
    } finally {
      removeLoggingAppenders();
    }
  }

  /**
   * Configure the Hadoop environment
   * 
   * @param shim
   *          Hadoop Shim
   * @param sqoopConfig
   *          Sqoop configuration settings
   * @param conf
   *          Hadoop configuration
   * @throws org.pentaho.di.core.exception.KettleException
   * 
   */
  public void configure( HadoopShim shim, S sqoopConfig, Configuration conf ) throws KettleException {
    try {
      List<String> messages = new ArrayList<String>();
      DatabaseMeta databaseMeta = parentJob.getJobMeta().findDatabase( sqoopConfig.getDatabase() );
      sqoopConfig.setConnectionInfo( environmentSubstitute( databaseMeta.getName() ),
          environmentSubstitute( databaseMeta.getURL() ), environmentSubstitute( databaseMeta.getUsername() ),
          environmentSubstitute( databaseMeta.getPassword() ) );
      shim.configureConnectionInformation( environmentSubstitute( sqoopConfig.getNamenodeHost() ),
          environmentSubstitute( sqoopConfig.getNamenodePort() ), environmentSubstitute( sqoopConfig
              .getJobtrackerHost() ), environmentSubstitute( sqoopConfig.getJobtrackerPort() ), conf, messages );
      for ( String m : messages ) {
        logBasic( m );
      }
    } catch ( Exception e ) {
      throw new KettleException( BaseMessages.getString( AbstractSqoopJobEntry.class,
          "ErrorConfiguringHadoopEnvironment" ), e );
    }
  }

  /**
   * Determine if a database type is supported.
   * 
   * @param databaseType
   *          Database type to check for compatibility
   * @return {@code true} if this database is supported for this tool
   */
  public boolean isDatabaseSupported( Class<? extends DatabaseInterface> databaseType ) {
    // For now all database types are supported
    return true;
  }
}
