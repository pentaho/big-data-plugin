/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package org.pentaho.big.data.services.bootstrap;

import org.pentaho.di.core.database.DatabaseInterface;
import org.pentaho.database.IDatabaseDialect;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.plugins.DatabasePluginType;
import org.pentaho.platform.engine.core.system.PentahoSystem;
import com.pentaho.big.data.bundles.impl.shim.hbase.HBaseServiceFactory;
import com.pentaho.big.data.bundles.impl.shim.hdfs.HadoopFileSystemFactoryImpl;
import com.pentaho.big.data.bundles.impl.shim.hive.HiveDriver;
import com.pentaho.big.data.bundles.impl.shim.hive.ImpalaDriver;
import com.pentaho.big.data.bundles.impl.shim.hive.ImpalaSimbaDriver;
import com.pentaho.big.data.bundles.impl.shim.hive.SparkSimbaDriver;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.logging.log4j.Logger;
import org.pentaho.authentication.mapper.api.AuthenticationMappingManager;
import org.pentaho.big.data.api.cluster.service.locator.impl.NamedClusterServiceLocatorImpl;
import org.pentaho.big.data.api.jdbc.impl.ClusterInitializingDriver;
import org.pentaho.big.data.api.jdbc.impl.DriverLocatorImpl;
import org.pentaho.big.data.api.jdbc.impl.JdbcUrlParserImpl;
import org.pentaho.big.data.api.shims.LegacyShimLocator;
import org.pentaho.big.data.hadoop.bootstrap.HadoopConfigurationBootstrap;
import org.pentaho.big.data.impl.cluster.tests.hdfs.GatewayListHomeDirectoryTest;
import org.pentaho.big.data.impl.cluster.tests.hdfs.GatewayListRootDirectoryTest;
import org.pentaho.big.data.impl.cluster.tests.hdfs.GatewayPingFileSystemEntryPoint;
import org.pentaho.big.data.impl.cluster.tests.hdfs.GatewayWriteToAndDeleteFromUsersHomeFolderTest;
import org.pentaho.big.data.impl.cluster.tests.kafka.KafkaConnectTest;
import org.pentaho.big.data.impl.cluster.tests.mr.GatewayPingJobTrackerTest;
import org.pentaho.big.data.impl.cluster.tests.oozie.GatewayPingOozieHostTest;
import org.pentaho.big.data.impl.cluster.tests.zookeeper.GatewayPingZookeeperEnsembleTest;
import org.pentaho.big.data.impl.shim.HadoopClientServicesFactory;
import org.pentaho.big.data.impl.shim.format.FormatServiceFactory;
import org.pentaho.big.data.impl.shim.mapreduce.MapReduceServiceFactoryImpl;
import org.pentaho.big.data.impl.shim.mapreduce.TransformationVisitorService;
import org.pentaho.big.data.impl.vfs.hdfs.AzureHdInsightsFileNameParser;
import org.pentaho.big.data.impl.vfs.hdfs.HDFSFileNameParser;
import org.pentaho.big.data.impl.vfs.hdfs.HDFSFileProvider;
import org.pentaho.big.data.impl.vfs.hdfs.MapRFileNameParser;
import org.pentaho.big.data.impl.vfs.hdfs.nc.NamedClusterProvider;
import org.pentaho.bigdata.api.hdfs.impl.HadoopFileSystemLocatorImpl;
import org.pentaho.di.core.bowl.DefaultBowl;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.hadoop.HadoopSpoonPlugin;
import org.pentaho.di.core.plugins.LifecyclePluginType;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.service.ServiceProvider;
import org.pentaho.di.core.service.ServiceProviderInterface;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.HadoopConfigurationLocator;
import org.pentaho.hadoop.shim.api.ConfigurationException;
import org.pentaho.hadoop.shim.api.core.ShimIdentifierInterface;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystemFactory;
import org.pentaho.hadoop.shim.api.jdbc.JdbcUrlParser;
import org.pentaho.hadoop.shim.api.services.BigDataServicesInitializer;
import org.pentaho.hadoop.shim.common.CommonFormatShim;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.hbase.shim.common.HBaseShimImpl;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.i18n.impl.BaseMessagesMessageGetterFactoryImpl;
import org.pentaho.runtime.test.impl.RuntimeTesterImpl;
import org.pentaho.runtime.test.network.impl.ConnectivityTestFactoryImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Level;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;

@ServiceProvider(id = "BigDataCEServiceInitializer", description = "", provides = BigDataServicesInitializer.class)
public class BigDataCEServiceInitializerImpl implements BigDataServicesInitializer, ServiceProviderInterface<BigDataServicesInitializer> {
  protected static final Logger logger = LogManager.getLogger( BigDataCEServiceInitializerImpl.class );
  private static final String LOGGING_PROPERTIES_FILE = "bigdata-logging.properties";
  private static final String LOGGER_PREFIX = "logger.";


  @Override
  public void doInitialize() {
    // Register loggers from properties file first
    registerLoggers();

    // Initialize Big Data logging configuration
    BigDataLogConfig.initializeBigDataLogging();

    logger.info( "Starting Pentaho Big Data Plugin bootstrap process." );
    try {
      HadoopShim hadoopShim = initializeCommonServices();
      if ( hadoopShim == null ) {
        return;
      }

      List<String> shimAvailableServices = hadoopShim.getAvailableServices();
      AuthenticationMappingManager authenticationMappingManager =
        initializeAuthenticationManager( hadoopShim, shimAvailableServices );
      HadoopFileSystemLocatorImpl hadoopFileSystemLocator =
        initializeHdfsServices( hadoopShim, shimAvailableServices, authenticationMappingManager );
      NamedClusterServiceLocatorImpl namedClusterServiceLocator = NamedClusterServiceLocatorImpl
        .getInstance( hadoopShim.getShimIdentifier().getId() );
      initializeFormatServices( hadoopShim, shimAvailableServices, namedClusterServiceLocator );
      initializeMapReduceServices( hadoopShim, shimAvailableServices, authenticationMappingManager,
        namedClusterServiceLocator );
      initializeSqoopServices( hadoopShim, shimAvailableServices, authenticationMappingManager,
        namedClusterServiceLocator );
      initializeHiveServices( hadoopShim, shimAvailableServices, authenticationMappingManager );
      initializeHBaseServices( hadoopShim, shimAvailableServices, authenticationMappingManager,
        namedClusterServiceLocator );
      initializeYarnServices( hadoopShim, shimAvailableServices, authenticationMappingManager,
        hadoopFileSystemLocator, namedClusterServiceLocator );
      initializeRuntimeTests( hadoopFileSystemLocator, namedClusterServiceLocator );
      registerBigDataDatabaseDialects();

    } catch ( ConfigurationException | ClassNotFoundException | IllegalAccessException | InstantiationException |
              KettlePluginException e ) {
      logger.error(
        "There was an error during the Pentaho Big Data Plugin bootstrap process. Some Big Data features may not be "
          + "available after startup.",
        e );
    }

    logger.info( "Finished Pentaho Big Data Plugin bootstrap process." );

  }

  /**
   * Register all loggers from the bigdata-logging.properties file.
   * Loggers are registered dynamically based on the file contents.
   */
  protected void registerLoggers() {
    logger.debug( "Registering Big Data loggers from {}", LOGGING_PROPERTIES_FILE );

    Properties props = new Properties();
    InputStream is = null;
    try {
      // Get the plugin interface to locate the plugin directory
      PluginInterface pluginInterface = PluginRegistry.getInstance()
        .findPluginWithId( LifecyclePluginType.class, HadoopSpoonPlugin.PLUGIN_ID );

      if ( pluginInterface == null ) {
        logger.warn( "Could not find Big Data plugin in registry - cannot load {}", LOGGING_PROPERTIES_FILE );
        return;
      }

      // Construct the path to the logging properties file in the plugin root directory
      FileObject pluginDir = KettleVFS.getInstance( DefaultBowl.getInstance() )
        .getFileObject( pluginInterface.getPluginDirectory().getPath() );
      FileObject loggingPropsFile = pluginDir.resolveFile( LOGGING_PROPERTIES_FILE );

      if ( !loggingPropsFile.exists() ) {
        logger.warn( "Could not find {} in plugin directory {} - no loggers will be registered",
          LOGGING_PROPERTIES_FILE, pluginDir.getName().getPath() );
        return;
      }

      is = loggingPropsFile.getContent().getInputStream();
      props.load( is );
      logger.debug( "Loaded logging configuration from {}", loggingPropsFile.getName().getPath() );

      int registeredCount = 0;
      for ( String propName : props.stringPropertyNames() ) {
        if ( propName.startsWith( LOGGER_PREFIX ) ) {
          String loggerName = propName.substring( LOGGER_PREFIX.length() );
          String levelStr = props.getProperty( propName );

          try {
            Level level = Level.toLevel( levelStr, Level.INFO );
            BigDataLogConfig.registerLogger( loggerName, level );
            logger.debug( "Registered logger: {} = {}", loggerName, level );
            registeredCount++;
          } catch ( Exception e ) {
            logger.warn( "Invalid log level '{}' for logger '{}', defaulting to INFO", levelStr, loggerName );
            BigDataLogConfig.registerLogger( loggerName, Level.INFO );
            registeredCount++;
          }
        }
      }
      logger.debug( "Registered {} Big Data loggers", registeredCount );
    } catch ( KettleException e ) {
      logger.error( "Error accessing plugin directory for {} - no loggers will be registered",
        LOGGING_PROPERTIES_FILE, e );
    } catch ( IOException e ) {
      logger.error( "Error loading {} - no loggers will be registered", LOGGING_PROPERTIES_FILE, e );
    } finally {
      if ( is != null ) {
        try {
          is.close();
        } catch ( IOException e ) {
          logger.warn( "Error closing input stream for {}", LOGGING_PROPERTIES_FILE, e );
        }
      }
    }
  }

  @Override
  public int getPriority() {
    return 100;
  }

  @Override
  public boolean useProxyWrap() {
    return true;
  }

  /**
   * Initialize common Hadoop services and configuration
   *
   * @return HadoopShim instance or null if no configuration found
   */
  protected HadoopShim initializeCommonServices() throws ConfigurationException {
    logger.debug( "Bootstrapping the Common Services." );
    HadoopConfigurationBootstrap hadoopConfigurationBootstrap = HadoopConfigurationBootstrap.getInstance();
    HadoopConfigurationLocator hadoopConfigurationProvider =
      ( HadoopConfigurationLocator ) hadoopConfigurationBootstrap.getProvider();
    if ( hadoopConfigurationProvider == null ) {
      logger.info( "No Hadoop active configuration found." );
      return null;
    }
    HadoopConfiguration hadoopConfiguration = hadoopConfigurationProvider.getActiveConfiguration();
    HadoopShim hadoopShim = hadoopConfiguration.getHadoopShim();

    // Add active shim to the Legacy Shim Locator
    List<ShimIdentifierInterface> registeredShims = new ArrayList<>();
    registeredShims.add( hadoopShim.getShimIdentifier() );
    LegacyShimLocator.getInstance().setRegisteredShims( registeredShims );

    return hadoopShim;
  }

  /**
   * Initialize authentication manager service
   *
   * @param hadoopShim            the Hadoop shim
   * @param shimAvailableServices list of available services
   * @return AuthenticationMappingManager instance or null
   */
  protected AuthenticationMappingManager initializeAuthenticationManager( HadoopShim hadoopShim,
                                                                          List<String> shimAvailableServices ) {
    logger.debug( "Bootstrapping the authentication manager service." );
    logger.debug( "No authentication manager service available in CE - continuing without authentication" );
    return null;
  }

  /**
   * Initialize HDFS services and file providers
   *
   * @param hadoopShim                   the Hadoop shim
   * @param shimAvailableServices        list of available services
   * @param authenticationMappingManager authentication manager
   * @return HadoopFileSystemLocatorImpl instance or null
   */
  protected HadoopFileSystemLocatorImpl initializeHdfsServices( HadoopShim hadoopShim,
                                                                List<String> shimAvailableServices,
                                                                AuthenticationMappingManager authenticationMappingManager ) {
    logger.debug( "Bootstrapping the HDFS Services." );
    HadoopFileSystemLocatorImpl hadoopFileSystemLocator = null;
    if ( shimAvailableServices.contains( "hdfs" ) ) {
      List<String> availableHdfsOptions = hadoopShim.getServiceOptions( "hdfs" );
      List<String> availableHdfsSchemas = hadoopShim.getAvailableHdfsSchemas();
      List<HadoopFileSystemFactory> hadoopFileSystemFactoryList = new ArrayList<>();

      if ( availableHdfsOptions.contains( "hdfs" ) ) {
        logger.debug( "Adding 'hdfs' factory." );
        HadoopFileSystemFactory hadoopFileSystemFactory =
          new HadoopFileSystemFactoryImpl( hadoopShim, hadoopShim.getShimIdentifier() );
        hadoopFileSystemFactoryList.add( hadoopFileSystemFactory );
      }

      hadoopFileSystemLocator = HadoopFileSystemLocatorImpl.getInstance();
      hadoopFileSystemLocator.setHadoopFileSystemFactories( hadoopFileSystemFactoryList );

      initializeHdfsSchemas( hadoopFileSystemLocator, availableHdfsSchemas );
    } else {
      logger.debug( "No HDFS Services defined." );
    }
    return hadoopFileSystemLocator;
  }

  /**
   * Initialize HDFS schemas (hdfs, maprfs, wasb, etc.)
   *
   * @param hadoopFileSystemLocator the file system locator
   * @param availableHdfsSchemas    list of available HDFS schemas
   */
  protected void initializeHdfsSchemas( HadoopFileSystemLocatorImpl hadoopFileSystemLocator,
                                        List<String> availableHdfsSchemas ) {
    if ( availableHdfsSchemas.contains( "hdfs" ) ) {
      logger.debug( "Adding 'hdfs' schema.'" );
      try {
        HDFSFileProvider hdfsHDFSFileProvider =
          new HDFSFileProvider( hadoopFileSystemLocator, "hdfs", HDFSFileNameParser.getInstance() );
      } catch ( FileSystemException e ) {
        throw new RuntimeException( e );
      }
    }
    if ( availableHdfsSchemas.contains( "maprfs" ) ) {
      logger.debug( "Adding 'maprfs' schema.'" );
      try {
        HDFSFileProvider maprfsHDFSFileProvider =
          new HDFSFileProvider( hadoopFileSystemLocator, "maprfs", MapRFileNameParser.getInstance() );
      } catch ( FileSystemException e ) {
        throw new RuntimeException( e );
      }
    }
    if ( availableHdfsSchemas.contains( "escalefs" ) ) {
      logger.debug( "Adding 'escalefs' schema.'" );
      try {
        HDFSFileProvider escalefsHDFSFileProvider =
          new HDFSFileProvider( hadoopFileSystemLocator, "escalefs", MapRFileNameParser.getInstance() );
      } catch ( FileSystemException e ) {
        throw new RuntimeException( e );
      }
    }
    if ( availableHdfsSchemas.contains( "wasb" ) ) {
      logger.debug( "Adding 'wasb' schema.'" );
      try {
        HDFSFileProvider wasbHDFSFileProvider =
          new HDFSFileProvider( hadoopFileSystemLocator, "wasb", AzureHdInsightsFileNameParser.getInstance() );
      } catch ( FileSystemException e ) {
        throw new RuntimeException( e );
      }
    }
    if ( availableHdfsSchemas.contains( "wasbs" ) ) {
      logger.debug( "Adding 'wasbs' schema.'" );
      try {
        HDFSFileProvider wasbsHDFSFileProvider =
          new HDFSFileProvider( hadoopFileSystemLocator, "wasbs", AzureHdInsightsFileNameParser.getInstance() );
      } catch ( FileSystemException e ) {
        throw new RuntimeException( e );
      }
    }
    if ( availableHdfsSchemas.contains( "abfs" ) ) {
      logger.debug( "Adding 'abfs' schema.'" );
      try {
        HDFSFileProvider abfsHDFSFileProvider =
          new HDFSFileProvider( hadoopFileSystemLocator, "abfs", AzureHdInsightsFileNameParser.getInstance() );
      } catch ( FileSystemException e ) {
        throw new RuntimeException( e );
      }
    }
    if ( availableHdfsSchemas.contains( "hc" ) ) {
      logger.debug( "Adding 'hc' schema.'" );
      try {
        NamedClusterProvider namedClusterProvider =
          new NamedClusterProvider( hadoopFileSystemLocator, "hc", HDFSFileNameParser.getInstance() );
      } catch ( FileSystemException e ) {
        throw new RuntimeException( e );
      }

      // Initialize UI NamedClusterProvider if available (Spoon environment)
      initializeUINamedClusterProvider();
    }
  }

  /**
   * Initialize format service factories (Parquet, ORC, Avro, etc.)
   *
   * @param hadoopShim                 the Hadoop shim
   * @param shimAvailableServices      list of available services
   * @param namedClusterServiceLocator the service locator
   */
  protected void initializeFormatServices( HadoopShim hadoopShim,
                                           List<String> shimAvailableServices,
                                           NamedClusterServiceLocatorImpl namedClusterServiceLocator ) {
    logger.debug( "Bootstrap the common format service factories." );
    if ( shimAvailableServices.contains( "common_formats" ) ) {
      CommonFormatShim commonFormatShim = new CommonFormatShim();
      FormatServiceFactory formatServiceFactory = new FormatServiceFactory( commonFormatShim );
      Map formatFactoryMap = new HashMap<String, String>();
      formatFactoryMap.put( "shim", hadoopShim.getShimIdentifier().getId() );
      formatFactoryMap.put( "service", "format" );
      namedClusterServiceLocator.factoryAdded( formatServiceFactory, formatFactoryMap );
    } else {
      logger.debug( "No common format service factories defined." );
    }
  }

  /**
   * Initialize MapReduce service factories
   *
   * @param hadoopShim                   the Hadoop shim
   * @param shimAvailableServices        list of available services
   * @param authenticationMappingManager authentication manager
   * @param namedClusterServiceLocator   the service locator
   */
  protected void initializeMapReduceServices( HadoopShim hadoopShim,
                                              List<String> shimAvailableServices,
                                              AuthenticationMappingManager authenticationMappingManager,
                                              NamedClusterServiceLocatorImpl namedClusterServiceLocator ) {
    logger.debug( "Bootstrap the mapreduce service factories." );
    if ( shimAvailableServices.contains( "mapreduce" ) ) {
      List<String> availableMapreduceOptions = hadoopShim.getServiceOptions( "mapreduce" );
      List<TransformationVisitorService> visitorServices = new ArrayList<>();
      Map mapReducefactoryMap = new HashMap<String, String>();
      mapReducefactoryMap.put( "shim", hadoopShim.getShimIdentifier().getId() );
      mapReducefactoryMap.put( "service", "mapreduce" );

      if ( availableMapreduceOptions.contains( "mapreduce" ) ) {
        logger.debug( "Adding 'mapreduce' factory." );
        MapReduceServiceFactoryImpl mapReduceServiceFactory = new MapReduceServiceFactoryImpl(
          hadoopShim,
          Executors.newCachedThreadPool(),
          visitorServices
        );
        namedClusterServiceLocator.factoryAdded( mapReduceServiceFactory, mapReducefactoryMap );
      }
    } else {
      logger.debug( "No mapreduce service factories defined." );
    }
  }

  /**
   * Initialize Sqoop (Hadoop client) service factories
   *
   * @param hadoopShim                   the Hadoop shim
   * @param shimAvailableServices        list of available services
   * @param authenticationMappingManager authentication manager
   * @param namedClusterServiceLocator   the service locator
   */
  protected void initializeSqoopServices( HadoopShim hadoopShim,
                                          List<String> shimAvailableServices,
                                          AuthenticationMappingManager authenticationMappingManager,
                                          NamedClusterServiceLocatorImpl namedClusterServiceLocator ) {
    logger.debug( "Bootstrap the hadoop client (Sqoop) service factories." );
    if ( shimAvailableServices.contains( "sqoop" ) ) {
      List<String> availableSqoopOptions = hadoopShim.getServiceOptions( "sqoop" );
      Map hadoopClientFactoryMap = new HashMap<String, String>();
      hadoopClientFactoryMap.put( "shim", hadoopShim.getShimIdentifier().getId() );
      hadoopClientFactoryMap.put( "service", "shimservices" );

      if ( availableSqoopOptions.contains( "sqoop" ) ) {
        logger.debug( "Adding 'sqoop' factory." );
        HadoopClientServicesFactory hadoopClientServicesFactory = new HadoopClientServicesFactory( hadoopShim );
        namedClusterServiceLocator.factoryAdded( hadoopClientServicesFactory, hadoopClientFactoryMap );
      }
    } else {
      logger.debug( "No hadoop client (Sqoop) service factories defined." );
    }
  }

  /**
   * Initialize Hive service drivers
   *
   * @param hadoopShim                   the Hadoop shim
   * @param shimAvailableServices        list of available services
   * @param authenticationMappingManager authentication manager
   */
  protected void initializeHiveServices( HadoopShim hadoopShim,
                                         List<String> shimAvailableServices,
                                         AuthenticationMappingManager authenticationMappingManager ) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    logger.debug( "Bootstrap the Hive services." );
    if ( shimAvailableServices.contains( "hive" ) ) {
      JdbcUrlParser jdbcUrlParser = JdbcUrlParserImpl.getInstance();
      DriverLocatorImpl driverLocator = DriverLocatorImpl.getInstance();
      int pentaho_jdbc_lazydrivers_num = 5;
      ClusterInitializingDriver clusterInitializingDriver = new ClusterInitializingDriver(
        jdbcUrlParser,
        driverLocator,
        pentaho_jdbc_lazydrivers_num );
      List<String> availableHiveDrivers = hadoopShim.getAvailableHiveDrivers();

      // Register CE Hive drivers
      registerHiveDrivers( hadoopShim, availableHiveDrivers, jdbcUrlParser, driverLocator );
    } else {
      logger.debug( "No Hive services defined." );
    }
  }

  /**
   * We use to register using the Activation.java class when in OSGi. We have to now register all big-data
   * dialect one by one using PentahoSystem.registerObject.  The reason to use reflection is that the bootstrap jar
   * in big-data plugin has its own classloader and pdi-hive-core.jar which contains all these dialect is not loaded
   * from this classloader
   */
  protected void registerBigDataDatabaseDialects() throws KettlePluginException {
    if ( PentahoSystem.getInitializedOK() ) {
      try {
        PluginRegistry registry = PluginRegistry.getInstance();
        PluginInterface plugin = registry.getPlugin( DatabasePluginType.class, "HIVE2" );
        //Register a DatabaseMeta in the Pentaho System
        Object hive2Meta = registry.loadClass( plugin );
        PentahoSystem.registerObject( hive2Meta, DatabaseInterface.class );
        //Register a dialect in the Pentaho System
        Class<IDatabaseDialect> hive2Dialect = registry.getClass( plugin, "org.pentaho.big.data.kettle.plugins.hive.Hive2DatabaseDialect" );
        PentahoSystem.registerObject( hive2Dialect.getDeclaredConstructor().newInstance(), IDatabaseDialect.class );

        plugin = registry.getPlugin( DatabasePluginType.class, "IMPALA" );
        //Register a DatabaseMeta in the Pentaho System
        Object impalaMeta = registry.loadClass( plugin );
        PentahoSystem.registerObject( impalaMeta, DatabaseInterface.class );
        //Register a dialect in the Pentaho System
        Class<IDatabaseDialect> impalaDialect = registry.getClass( plugin, "org.pentaho.big.data.kettle.plugins.hive.ImpalaDatabaseDialect" );
        PentahoSystem.registerObject( impalaDialect.getDeclaredConstructor().newInstance(), IDatabaseDialect.class );

        plugin = registry.getPlugin( DatabasePluginType.class, "IMPALASIMBA" );
        //Register a DatabaseMeta in the Pentaho System
        Object impalaSimbaMeta = registry.loadClass( plugin );
        PentahoSystem.registerObject( impalaSimbaMeta, DatabaseInterface.class );
        //Register a dialect in the Pentaho System
        Class<IDatabaseDialect> impalaSimbaDialect = registry.getClass( plugin, "org.pentaho.big.data.kettle.plugins.hive.ImpalaSimbaDatabaseDialect" );
        PentahoSystem.registerObject( impalaSimbaDialect.getDeclaredConstructor().newInstance(), IDatabaseDialect.class );

        plugin = registry.getPlugin( DatabasePluginType.class, "SPARKSIMBA" );
        //Register a DatabaseMeta in the Pentaho System
        Object sparkSimbaMeta = registry.loadClass( plugin );
        PentahoSystem.registerObject( sparkSimbaMeta, DatabaseInterface.class );
        //Register a dialect in the Pentaho System
        Class<IDatabaseDialect> sparkSimbaDialect = registry.getClass( plugin, "org.pentaho.big.data.kettle.plugins.hive.SparkSimbaDatabaseDialect" );
        PentahoSystem.registerObject( sparkSimbaDialect.getDeclaredConstructor().newInstance(), IDatabaseDialect.class );
      } catch ( NoSuchMethodException e ) {
        throw new RuntimeException( e );
      } catch ( InvocationTargetException e ) {
        throw new RuntimeException( e );
      } catch ( InstantiationException e ) {
        throw new RuntimeException( e );
      } catch ( IllegalAccessException e ) {
        throw new RuntimeException( e );
      }
    }
  }

  /**
   * Register Community Edition Hive drivers
   *
   * @param hadoopShim           the Hadoop shim
   * @param availableHiveDrivers list of available Hive drivers
   * @param jdbcUrlParser        JDBC URL parser
   * @param driverLocator        driver locator
   */
  protected void registerHiveDrivers( HadoopShim hadoopShim,
                                      List<String> availableHiveDrivers,
                                      JdbcUrlParser jdbcUrlParser,
                                      DriverLocatorImpl driverLocator ) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    if ( availableHiveDrivers.contains( "hive" ) ) {
      logger.debug( "Adding 'hive' driver." );
      HiveDriver hiveDriver = null;
      hiveDriver = new HiveDriver(
        jdbcUrlParser,
        "org.apache.hive.jdbc.HiveDriver",
        hadoopShim.getShimIdentifier().getId() );
      driverLocator.registerDriver( hiveDriver );
    }
    if ( availableHiveDrivers.contains( "impala" ) ) {
      logger.debug( "Adding 'impala' driver." );
      ImpalaDriver impalaDriver = null;
      impalaDriver = new ImpalaDriver(
        jdbcUrlParser,
        "com.cloudera.impala.jdbc41.Driver",
        hadoopShim.getShimIdentifier().getId() );
      driverLocator.registerDriver( impalaDriver );
    }
    if ( availableHiveDrivers.contains( "impala_simba" ) ) {
      logger.debug( "Adding 'impala_simba' driver." );
      ImpalaSimbaDriver impalaSimbaDriver = null;
      impalaSimbaDriver = new ImpalaSimbaDriver(
        jdbcUrlParser,
        "com.cloudera.impala.jdbc41.Driver",
        hadoopShim.getShimIdentifier().getId() );
      driverLocator.registerDriver( impalaSimbaDriver );
    }
    if ( availableHiveDrivers.contains( "spark_simba" ) ) {
      logger.debug( "Adding 'spark_simba' driver." );
      SparkSimbaDriver sparkSimbaDriver = null;
      sparkSimbaDriver = new SparkSimbaDriver(
        jdbcUrlParser,
        "org.apache.hive.jdbc.HiveDriver",
        hadoopShim.getShimIdentifier().getId() );
      driverLocator.registerDriver( sparkSimbaDriver );
    }
  }

  /**
   * Initialize HBase service factories
   *
   * @param hadoopShim                   the Hadoop shim
   * @param shimAvailableServices        list of available services
   * @param authenticationMappingManager authentication manager
   * @param namedClusterServiceLocator   the service locator
   */
  protected void initializeHBaseServices( HadoopShim hadoopShim,
                                          List<String> shimAvailableServices,
                                          AuthenticationMappingManager authenticationMappingManager,
                                          NamedClusterServiceLocatorImpl namedClusterServiceLocator ) throws ConfigurationException {
    logger.debug( "Bootstrap the HBase services." );
    if ( shimAvailableServices.contains( "hbase" ) ) {
      List<String> availableHbaseOptions = hadoopShim.getServiceOptions( "hbase" );
      HBaseShimImpl hBaseShim = new HBaseShimImpl();
      Map hBaseServiceFactoryMap = new HashMap<String, String>();
      hBaseServiceFactoryMap.put( "shim", hadoopShim.getShimIdentifier().getId() );
      hBaseServiceFactoryMap.put( "service", "hbase" );

      if ( availableHbaseOptions.contains( "hbase" ) ) {
        logger.debug( "Adding 'hbase' factory." );
        HBaseServiceFactory hBaseServiceFactory = new HBaseServiceFactory( hBaseShim );
        namedClusterServiceLocator.factoryAdded( hBaseServiceFactory, hBaseServiceFactoryMap );
      }
    } else {
      logger.debug( "No HBase services defined." );
    }
  }

  /**
   * Initialize Yarn service factories
   *
   * @param hadoopShim                   the Hadoop shim
   * @param shimAvailableServices        list of available services
   * @param authenticationMappingManager authentication manager
   * @param hadoopFileSystemLocator      file system locator
   * @param namedClusterServiceLocator   the service locator
   */
  protected void initializeYarnServices( HadoopShim hadoopShim,
                                         List<String> shimAvailableServices,
                                         AuthenticationMappingManager authenticationMappingManager,
                                         HadoopFileSystemLocatorImpl hadoopFileSystemLocator,
                                         NamedClusterServiceLocatorImpl namedClusterServiceLocator ) {
    logger.debug( "Bootstrap the Yarn services." );
    // CE version does not support Yarn - EE version will override
    logger.debug( "No Yarn services available in CE" );
  }

  /**
   * Initialize UI NamedClusterProvider for Spoon environment
   * This provider is optional and only available in UI contexts
   */
  protected void initializeUINamedClusterProvider() {
    try {
      // Try to instantiate the UI NamedClusterProvider (browse plugin)
      org.pentaho.big.data.impl.browse.NamedClusterProvider uiProvider =
        new org.pentaho.big.data.impl.browse.NamedClusterProvider();
      logger.debug( "UI NamedClusterProvider initialized successfully for Spoon environment" );
    } catch ( NoClassDefFoundError | Exception e ) {
      logger.debug(
        "The UI NamedClusterProvider could not be instantiated. This is OK for Pentaho Server but it should be "
          + "examined for Spoon." );
    }
  }

  /**
   * Initialize runtime tests for cluster connectivity and health checks
   *
   * @param hadoopFileSystemLocator    file system locator
   * @param namedClusterServiceLocator the service locator
   */
  protected void initializeRuntimeTests( HadoopFileSystemLocatorImpl hadoopFileSystemLocator,
                                         NamedClusterServiceLocatorImpl namedClusterServiceLocator ) {
    logger.debug( "Bootstrap the run time tests." );
    RuntimeTester runtimeTester = RuntimeTesterImpl.getInstance();
    runtimeTester.addRuntimeTest(
      new GatewayPingFileSystemEntryPoint( BaseMessagesMessageGetterFactoryImpl.getInstance(),
        new ConnectivityTestFactoryImpl() ) );
    runtimeTester.addRuntimeTest( new GatewayPingJobTrackerTest( BaseMessagesMessageGetterFactoryImpl.getInstance(),
      new ConnectivityTestFactoryImpl() ) );
    runtimeTester.addRuntimeTest( new GatewayPingOozieHostTest( BaseMessagesMessageGetterFactoryImpl.getInstance(),
      new ConnectivityTestFactoryImpl() ) );
    runtimeTester.addRuntimeTest(
      new GatewayPingZookeeperEnsembleTest( BaseMessagesMessageGetterFactoryImpl.getInstance(),
        new ConnectivityTestFactoryImpl() ) );
    runtimeTester.addRuntimeTest(
      new GatewayListRootDirectoryTest( BaseMessagesMessageGetterFactoryImpl.getInstance(),
        new ConnectivityTestFactoryImpl(), hadoopFileSystemLocator ) );
    runtimeTester.addRuntimeTest(
      new GatewayListHomeDirectoryTest( BaseMessagesMessageGetterFactoryImpl.getInstance(),
        new ConnectivityTestFactoryImpl(), hadoopFileSystemLocator ) );
    runtimeTester.addRuntimeTest(
      new GatewayWriteToAndDeleteFromUsersHomeFolderTest( BaseMessagesMessageGetterFactoryImpl.getInstance(),
        hadoopFileSystemLocator ) );
    runtimeTester.addRuntimeTest(
      new KafkaConnectTest( BaseMessagesMessageGetterFactoryImpl.getInstance(), namedClusterServiceLocator ) );
  }


}
