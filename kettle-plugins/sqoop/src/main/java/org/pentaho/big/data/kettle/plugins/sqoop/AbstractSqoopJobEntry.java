/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.sqoop;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.pentaho.big.data.kettle.plugins.job.PropertyEntry;
import org.pentaho.hadoop.shim.api.HadoopClientServices;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.big.data.kettle.plugins.job.AbstractJobEntry;
import org.pentaho.big.data.kettle.plugins.job.JobEntryMode;
import org.pentaho.big.data.kettle.plugins.job.JobEntryUtils;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.database.DatabaseInterface;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;
import org.w3c.dom.Node;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Properties;

/**
 * Base class for all Sqoop job entries.
 */
public abstract class AbstractSqoopJobEntry<S extends SqoopConfig> extends AbstractJobEntry<S> implements Cloneable,
    JobEntryInterface {

  private final String NamedClusterNameProperty = "pentahoNamedCluster";
  private final NamedClusterService namedClusterService;
  private final NamedClusterServiceLocator namedClusterServiceLocator;
  private final RuntimeTestActionService runtimeTestActionService;
  private final RuntimeTester runtimeTester;

  private DatabaseMeta usedDbConnection;

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
  private String[] LOGS_TO_MONITOR = new String[] { "org.apache.sqoop", "org.apache.hadoop", "com.pentaho.big.data.bundles.impl.shim.sqoop.knox" };

  /**
   * Cache for the levels of loggers we changed so we can revert them when we remove our appender
   */
  private final Map<String, Level> logLevelCache;

  /**
   * Declare the Sqoop tool used in this job entry.
   * 
   * @return the name of the sqoop tool to use, e.g. "import"
   */
  protected abstract String getToolName();

  protected AbstractSqoopJobEntry( NamedClusterService namedClusterService,
                                   NamedClusterServiceLocator namedClusterServiceLocator,
                                   RuntimeTestActionService runtimeTestActionService, RuntimeTester runtimeTester ) {
    this.namedClusterService = namedClusterService;
    this.namedClusterServiceLocator = namedClusterServiceLocator;
    this.runtimeTestActionService = runtimeTestActionService;
    this.runtimeTester = runtimeTester;
    logLevelCache = Maps.newHashMap();
  }

  @Override
  public void loadXML( Node node, List<DatabaseMeta> databaseMetas, List<SlaveServer> slaveServers,
      Repository repository ) throws KettleXMLException {
    super.loadXML( node, databaseMetas, slaveServers, repository );
    loadUsedDataBaseConnection( databaseMetas, getJobConfig() );

    if ( !loadNamedCluster( metaStore ) ) {
      // load default values for cluster & legacy fallback
      getJobConfig().loadClusterConfig( node );
    }
  }

  @Override
  public void loadRep( Repository rep, ObjectId id_jobentry, List<DatabaseMeta> databases,
      List<SlaveServer> slaveServers ) throws KettleException {
    super.loadRep( rep, id_jobentry, databases, slaveServers );
    loadUsedDataBaseConnection( databases, getJobConfig() );
    if ( !loadNamedCluster( metaStore ) ) {
      // load default values for cluster & legacy fallback
      try {
        getJobConfig().loadClusterConfig( rep, id_jobentry );
      } catch ( KettleException ke ) {
        logError( ke.getMessage(), ke );
      }
    }
  }

  @Override public String getXML() {
    return super.getXML() + getJobConfig().getClusterXML();
  }

  @Override public void saveRep( Repository rep, ObjectId id_job ) throws KettleException {
    super.saveRep( rep, id_job );
    getJobConfig().saveClusterConfig( rep, id_job, this );
  }

  private boolean loadNamedCluster( IMetaStore metaStore ) {
    try {
      // attempt to load from named cluster
      String clusterName = getJobConfig().getClusterName();

      return loadNamedCluster( clusterName );
    } catch ( Throwable t ) {
      logDebug( t.getMessage(), t );
    }
    return false;
  }

  private boolean loadNamedCluster( String clusterName ) {
    try {
      // load from system first, then fall back to copy stored with job (AbstractMeta)
      NamedCluster namedCluster = null;
      if ( !Strings.isNullOrEmpty( clusterName ) && namedClusterService.contains( clusterName, metaStore ) ) {
        // pull config from NamedCluster
        namedCluster = namedClusterService.read( clusterName, metaStore );
      }
      if ( namedCluster != null ) {
        getJobConfig().setNamedCluster( namedCluster );
        return true;
      }
    } catch ( Throwable t ) {
      logDebug( t.getMessage(), t );
    }
    return false;
  }

  @VisibleForTesting
  void loadUsedDataBaseConnection( List<DatabaseMeta> databases, S config ) {
    String database = config.getDatabase();
    DatabaseMeta databaseMeta = DatabaseMeta.findDatabase( databases, database );
    setUsedDbConnection( databaseMeta );
    if ( database == null ) {
      // sync up the advanced configuration if no database type is set
      config.copyConnectionInfoToAdvanced();
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
        stdErrProxy = new LoggingProxy( System.err, sqoopLogger, Level.INFO );
        System.setErr( stdErrProxy );
      }
      JobEntryUtils.attachAppenderTo( sqoopToKettleAppender, getLogLevel(), logLevelCache, LOGS_TO_MONITOR );
    } catch ( Exception ex ) {
      logMinimal( getString( "ErrorAttachingLogging" ) );
      logDebug( Throwables.getStackTraceAsString( ex ) );

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
      logError( getString( "ErrorDetachingLogging" ) );
      logDebug( Throwables.getStackTraceAsString( ex ) );
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
      warnings.add( getString( "ValidationError.Connect.Message", config.getConnect() ) );
    }

    try {
      JobEntryUtils.asLong( config.getBlockingPollingInterval(), variables );
    } catch ( NumberFormatException ex ) {
      warnings.add( getString(
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
    logError( getString( "ErrorRunningSqoopTool" ), e );
    removeLoggingAppenders();
    setJobResultFailed( jobResult );
  }

  @Override
  protected Runnable getExecutionRunnable( final Result jobResult ) throws KettleException {
    return new Runnable() {

      @Override public void run() {
        executeSqoop( jobResult );
      }
    };
  }

  /**
   * Executes Sqoop using the provided configuration objects. The {@code jobResult} will accurately reflect the
   * completed execution state when finished.
   * @param jobResult
   *          Result to update based on feedback from the Sqoop tool
   */
  protected void executeSqoop( Result jobResult ) {
    S config = getJobConfig();
    Properties properties = new Properties();

    attachLoggingAppenders();
    try {
      configure( config, properties );
      List<String> args = SqoopUtils.getCommandLineArgs( config, getVariables() );
      args.add( 0, getToolName() ); // push the tool command-line argument on the top of the args list

      if ( !loadNamedCluster( getMetaStore() ) ) {
        PropertyEntry entry = config.getCustomArguments().stream()
                .filter( p-> p.getKey() != null && p.getKey().equals( NamedClusterNameProperty ) )
                .findFirst()
                .orElse( null );
        if ( entry != null ) {
          loadNamedCluster( entry.getValue() );
        }
      }

      // Clone named cluster and copy in variable space
      NamedCluster namedCluster = config.getNamedCluster().clone();
      namedCluster.copyVariablesFrom( this );

      HadoopClientServices hadoopClientServices = namedClusterServiceLocator.getService( namedCluster, HadoopClientServices.class );

      int result = hadoopClientServices.runSqoop( args, properties );
      if ( result != 0 ) {
        setJobResultFailed( jobResult );
      }
    } catch ( Exception ex ) {
      logError( getString( "ErrorRunningSqoopTool" ), ex );
      setJobResultFailed( jobResult );
    } finally {
      removeLoggingAppenders();
    }
  }

  /**
   * Configure the Hadoop environment
   *
   * @param sqoopConfig
   *          Sqoop configuration settings
   * @param properties
   *          Execution properties
   * @throws KettleException
   *
   */
  public void configure( S sqoopConfig, Properties properties ) throws KettleException {
    configureDatabase( sqoopConfig );
  }

  /**
   * Configure database connection information
   *
   * @param sqoopConfig - Sqoop configuration
   */
  public void configureDatabase( S sqoopConfig ) throws KettleException {
    DatabaseMeta databaseMeta = getParentJob().getJobMeta().findDatabase( sqoopConfig.getDatabase() );

    // if databaseMeta == null we assume "USE_ADVANCED_MODE" is selected on QUICK_SETUP
    if ( sqoopConfig.getModeAsEnum() == JobEntryMode.QUICK_SETUP && databaseMeta != null ) {
      sqoopConfig.setConnectionInfo(
          databaseMeta.environmentSubstitute( databaseMeta.getName() ),
          databaseMeta.environmentSubstitute( databaseMeta.getURL() ),
          databaseMeta.environmentSubstitute( databaseMeta.getUsername() ),
          Encr.decryptPasswordOptionallyEncrypted( databaseMeta.environmentSubstitute( databaseMeta.getPassword() ) ) );
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

  @Override
  public DatabaseMeta[] getUsedDatabaseConnections() {
    return new DatabaseMeta[] { usedDbConnection, };
  }

  public DatabaseMeta getUsedDbConnection() {
    return usedDbConnection;
  }

  public void setUsedDbConnection( DatabaseMeta usedDbConnection ) {
    this.usedDbConnection = usedDbConnection;
  }

  @VisibleForTesting
  protected void setLogChannel( LogChannelInterface logChannel ) {
    this.log = logChannel;
  }

  public NamedClusterService getNamedClusterService() {
    return namedClusterService;
  }

  public RuntimeTestActionService getRuntimeTestActionService() {
    return runtimeTestActionService;
  }

  public RuntimeTester getRuntimeTester() {
    return runtimeTester;
  }

  private static String getString( String key, String... parameters ) {
    return BaseMessages.getString( AbstractSqoopJobEntry.class, key, parameters );
  }
}
