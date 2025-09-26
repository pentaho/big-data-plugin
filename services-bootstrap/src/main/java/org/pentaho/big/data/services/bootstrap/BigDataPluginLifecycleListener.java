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

import com.pentaho.big.data.bundles.impl.shim.hbase.HBaseServiceFactory;
import com.pentaho.big.data.bundles.impl.shim.hive.HiveDriver;
import com.pentaho.big.data.bundles.impl.shim.hive.ImpalaDriver;
import com.pentaho.big.data.bundles.impl.shim.hive.ImpalaSimbaDriver;
import com.pentaho.big.data.bundles.impl.shim.hive.SparkSimbaDriver;
import org.pentaho.authentication.mapper.api.AuthenticationMappingManager;
import org.pentaho.big.data.api.cluster.service.locator.impl.NamedClusterServiceLocatorImpl;
import org.pentaho.big.data.api.jdbc.impl.ClusterInitializingDriver;
import org.pentaho.big.data.api.jdbc.impl.DriverLocatorImpl;
import org.pentaho.big.data.api.jdbc.impl.JdbcUrlParserImpl;
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
import org.pentaho.hadoop.shim.api.HadoopClientServices;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceFactory;
import org.pentaho.hadoop.shim.api.mapreduce.MapReduceService;
import org.pentaho.big.data.impl.vfs.hdfs.AzureHdInsightsFileNameParser;
import org.pentaho.big.data.impl.vfs.hdfs.HDFSFileNameParser;
import org.pentaho.big.data.impl.vfs.hdfs.MapRFileNameParser;
import org.pentaho.big.data.impl.vfs.hdfs.nc.NamedClusterProvider;
import org.pentaho.di.core.annotations.KettleLifecyclePlugin;
import org.pentaho.di.core.lifecycle.KettleLifecycleListener;
import org.pentaho.di.core.lifecycle.LifecycleException;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.hadoop.shim.HadoopConfigurationLocator;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystemFactory;
import com.pentaho.big.data.bundles.impl.shim.hdfs.HadoopFileSystemFactoryImpl;
import org.pentaho.bigdata.api.hdfs.impl.HadoopFileSystemLocatorImpl;
import org.pentaho.hadoop.shim.api.ConfigurationException;
import org.pentaho.big.data.impl.vfs.hdfs.HDFSFileProvider;
import org.pentaho.hadoop.shim.api.jdbc.JdbcUrlParser;
import org.pentaho.hadoop.shim.common.CommonFormatShim;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.hbase.shim.common.HBaseShimImpl;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.i18n.impl.BaseMessagesMessageGetterFactoryImpl;
import org.pentaho.runtime.test.impl.RuntimeTesterImpl;
import org.pentaho.runtime.test.network.impl.ConnectivityTestFactoryImpl;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

@KettleLifecyclePlugin( id = "BigDataPlugin", name = "Big Data Plugin" )
public class BigDataPluginLifecycleListener implements KettleLifecycleListener {

  private static final Logger logger = LoggerFactory.getLogger( BigDataPluginLifecycleListener.class );

  @Override
  public void onEnvironmentInit() throws LifecycleException {
    logger.debug( "Starting Pentaho Big Data Plugin bootstrap process." );
    try {
      //////////////////////////////////////////////////////////////////////////////////
      /// Bootstrapping the Common Services
      //////////////////////////////////////////////////////////////////////////////////
      logger.debug( "Bootstrapping the Common Services." );
      HadoopConfigurationBootstrap hadoopConfigurationBootstrap = HadoopConfigurationBootstrap.getInstance();
      HadoopConfigurationLocator hadoopConfigurationProvider =
        (HadoopConfigurationLocator) hadoopConfigurationBootstrap.getProvider();
      if ( hadoopConfigurationProvider == null ) {
        logger.info( "No Hadoop active configuration found." );
        return;
      }
      HadoopConfiguration hadoopConfiguration = hadoopConfigurationProvider.getActiveConfiguration();
      HadoopShim hadoopShim = hadoopConfiguration.getHadoopShim();
      List<String> shimAvailableServices = hadoopShim.getAvailableServices();

      //////////////////////////////////////////////////////////////////////////////////
      /// Bootstrapping the authentication manager service
      //////////////////////////////////////////////////////////////////////////////////
      logger.debug( "Bootstrapping the authentication manager service." );
      AuthenticationMappingManager authenticationMappingManager = null;
      if ( shimAvailableServices.contains( "auth_manager" ) ) {
        // Use reflection to load EE authentication services if available
        authenticationMappingManager = EEServiceReflectionLoader.loadAuthenticationManager( hadoopShim );
        if ( authenticationMappingManager != null ) {
          logger.debug( "EE authentication manager loaded successfully via reflection" );
        } else {
          logger.debug( "EE authentication manager not available - continuing in CE mode" );
        }
      } else {
        logger.debug( "No authentication manager service defined in shim" );
      }

      //////////////////////////////////////////////////////////////////////////////////
      /// Bootstrapping the HDFS Services
      //////////////////////////////////////////////////////////////////////////////////
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

        // Load EE HDFS factories using reflection if available
        List<HadoopFileSystemFactory> eeHdfsFactories = EEServiceReflectionLoader.loadEEHDFSFactories(
          hadoopShim,
          availableHdfsOptions,
          authenticationMappingManager
        );
        hadoopFileSystemFactoryList.addAll( eeHdfsFactories );

        hadoopFileSystemLocator = HadoopFileSystemLocatorImpl.getInstance();
        hadoopFileSystemLocator.setHadoopFileSystemFactories( hadoopFileSystemFactoryList );

        if ( availableHdfsSchemas.contains( "hdfs" ) ) {
          logger.debug( "Adding 'hdfs' schema.'" );
          HDFSFileProvider hdfsHDFSFileProvider =
            new HDFSFileProvider( hadoopFileSystemLocator, "hdfs", HDFSFileNameParser.getInstance() );
        }
        // schema=maprfs
        if ( availableHdfsSchemas.contains( "maprfs" ) ) {
          logger.debug( "Adding 'maprfs' schema.'" );
          HDFSFileProvider maprfsHDFSFileProvider =
            new HDFSFileProvider( hadoopFileSystemLocator, "maprfs", MapRFileNameParser.getInstance() );
        }
        // schema=escalefs
        if ( availableHdfsSchemas.contains( "escalefs" ) ) {
          logger.debug( "Adding 'escalefs' schema.'" );
          HDFSFileProvider escalefsHDFSFileProvider =
            new HDFSFileProvider( hadoopFileSystemLocator, "escalefs", MapRFileNameParser.getInstance() );
        }
        // schema=wasb
        if ( availableHdfsSchemas.contains( "wasb" ) ) {
          logger.debug( "Adding 'wasb' schema.'" );
          HDFSFileProvider wasbHDFSFileProvider =
            new HDFSFileProvider( hadoopFileSystemLocator, "wasb", AzureHdInsightsFileNameParser.getInstance() );
        }
        // schema=wasbs
        if ( availableHdfsSchemas.contains( "wasbs" ) ) {
          logger.debug( "Adding 'wasbs' schema.'" );
          HDFSFileProvider wasbsHDFSFileProvider =
            new HDFSFileProvider( hadoopFileSystemLocator, "wasbs", AzureHdInsightsFileNameParser.getInstance() );
        }
        // schema=abfs
        if ( availableHdfsSchemas.contains( "abfs" ) ) {
          logger.debug( "Adding 'abfs' schema.'" );
          HDFSFileProvider abfsHDFSFileProvider =
            new HDFSFileProvider( hadoopFileSystemLocator, "abfs", AzureHdInsightsFileNameParser.getInstance() );
        }
        // schema=hc
        if ( availableHdfsSchemas.contains( "hc" ) ) {
          logger.debug( "Adding 'hc' schema.'" );
          NamedClusterProvider namedClusterProvider =
            new NamedClusterProvider( hadoopFileSystemLocator, "hc", HDFSFileNameParser.getInstance() );
          String uiNamedClusterProvider = "org.pentaho.big.data.impl.browse.NamedClusterProvider";
          try {
            Class<?> clazz = Class.forName( uiNamedClusterProvider );
            Object instance = clazz.getDeclaredConstructor().newInstance();
          } catch ( ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException |
                    java.lang.reflect.InvocationTargetException e ) {
            logger.debug(
              "The NamedClusterProvider could not be instantiated. This is OK for Pentaho Server but it should be "
                + "examined for Spoon." );
          }
        }
      } else {
        logger.debug( "No HDFS Services defined." );
      }

      //////////////////////////////////////////////////////////////////////////////////
      /// Bootstrap the common format service factories
      //////////////////////////////////////////////////////////////////////////////////
      logger.debug( "Bootstrap the common format service factories." );
      NamedClusterServiceLocatorImpl namedClusterServiceLocator = NamedClusterServiceLocatorImpl.getInstance();
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
      //////////////////////////////////////////////////////////////////////////////////
      /// Bootstrap the mapreduce service factories
      //////////////////////////////////////////////////////////////////////////////////
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

        // Load EE MapReduce factories using reflection if available
        List<NamedClusterServiceFactory<MapReduceService>> eeMapReduceFactories =
          EEServiceReflectionLoader.loadEEMapReduceFactories(
            hadoopShim,
            availableMapreduceOptions,
            authenticationMappingManager,
            Executors.newCachedThreadPool(),
            visitorServices
          );
        for ( NamedClusterServiceFactory<MapReduceService> eeFactory : eeMapReduceFactories ) {
          namedClusterServiceLocator.factoryAdded( eeFactory, mapReducefactoryMap );
        }
      } else {
        logger.debug( "No mapreduce service factories defined." );
      }

      //////////////////////////////////////////////////////////////////////////////////
      /// Bootstrap the hadoop client (Sqoop) service factories
      //////////////////////////////////////////////////////////////////////////////////
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

        // Load EE Sqoop factories using reflection if available
        if ( availableSqoopOptions.contains( "sqoop_impersonation" ) ) {
          logger.debug( "Loading EE Sqoop factories via reflection" );
          List<NamedClusterServiceFactory<HadoopClientServices>> eeFactories =
            EEServiceReflectionLoader.loadEESqoopFactories( hadoopShim, authenticationMappingManager );

          for ( NamedClusterServiceFactory<HadoopClientServices> factory : eeFactories ) {
            namedClusterServiceLocator.factoryAdded( factory, hadoopClientFactoryMap );
            logger.debug( "Successfully added EE Sqoop factory via reflection" );
          }
        }
      } else {
        logger.debug( "No hadoop client (Sqoop) service factories defined." );
      }

      //////////////////////////////////////////////////////////////////////////////////
      /// Bootstrap the Hive services
      //////////////////////////////////////////////////////////////////////////////////
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
        // Hive
        if ( availableHiveDrivers.contains( "hive" ) ) {
          logger.debug( "Adding 'hive' driver." );
          HiveDriver hiveDriver = new HiveDriver(
            jdbcUrlParser,
            "org.apache.hive.jdbc.HiveDriver",
            hadoopShim.getShimIdentifier().getId() );
          driverLocator.registerDriver( hiveDriver );
        }
        // Impala
        if ( availableHiveDrivers.contains( "impala" ) ) {
          logger.debug( "Adding 'impala' driver." );
          ImpalaDriver impalaDriver = new ImpalaDriver(
            jdbcUrlParser,
            "com.cloudera.impala.jdbc41.Driver",
            hadoopShim.getShimIdentifier().getId() );
          driverLocator.registerDriver( impalaDriver );
        }
        // ImpalaSimba
        if ( availableHiveDrivers.contains( "impala_simba" ) ) {
          logger.debug( "Adding 'impala_simba' driver." );
          ImpalaSimbaDriver impalaSimbaDriver = new ImpalaSimbaDriver(
            jdbcUrlParser,
            "com.cloudera.impala.jdbc41.Driver",
            hadoopShim.getShimIdentifier().getId() );
          driverLocator.registerDriver( impalaSimbaDriver );
        }
        // SparkSimba
        if ( availableHiveDrivers.contains( "spark_simba" ) ) {
          logger.debug( "Adding 'spark_simba' driver." );
          SparkSimbaDriver sparkSimbaDriver = new SparkSimbaDriver(
            jdbcUrlParser,
            "org.apache.hive.jdbc.HiveDriver",
            hadoopShim.getShimIdentifier().getId() );
          driverLocator.registerDriver( sparkSimbaDriver );
        }
        // Impersonated services
        // Load EE Hive drivers via reflection if available
        logger.debug( "Loading EE Hive drivers via reflection" );
        List<Object> eeHiveDrivers = EEServiceReflectionLoader.loadEEHiveDrivers(
          hadoopShim,
          availableHiveDrivers,
          jdbcUrlParser,
          authenticationMappingManager
        );

        // Register all EE Hive drivers
        for ( Object eeDriver : eeHiveDrivers ) {
          try {
            // Cast to ClusterInitializingDriver interface that all drivers implement
            driverLocator.registerDriver( (org.pentaho.big.data.api.jdbc.impl.ClusterInitializingDriver) eeDriver );
            logger.debug( "Successfully registered EE Hive driver: " + eeDriver.getClass().getSimpleName() );
          } catch ( Exception e ) {
            logger.warn( "Failed to register EE Hive driver: " + eeDriver.getClass().getName(), e );
          }
        }
      } else {
        logger.debug( "No Hive services defined." );
      }

      //////////////////////////////////////////////////////////////////////////////////
      /// Bootstrap the HBase services
      //////////////////////////////////////////////////////////////////////////////////
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

        // Load EE HBase factories via reflection if available
        logger.debug( "Loading EE HBase factories via reflection" );
        List<NamedClusterServiceFactory<?>> eeHBaseFactories = EEServiceReflectionLoader.loadEEHBaseFactories(
          hBaseShim,
          availableHbaseOptions,
          authenticationMappingManager
        );

        // Register all EE HBase factories
        for ( NamedClusterServiceFactory<?> eeFactory : eeHBaseFactories ) {
          try {
            namedClusterServiceLocator.factoryAdded( eeFactory, hBaseServiceFactoryMap );
            logger.debug( "Successfully registered EE HBase factory: " + eeFactory.getClass().getSimpleName() );
          } catch ( Exception e ) {
            logger.warn( "Failed to register EE HBase factory: " + eeFactory.getClass().getName(), e );
          }
        }
      } else {
        logger.debug( "No HBase services defined." );
      }

      //////////////////////////////////////////////////////////////////////////////////
      /// Bootstrap the Yarn services
      //////////////////////////////////////////////////////////////////////////////////
      logger.debug( "Bootstrap the Yarn services." );
      if ( shimAvailableServices.contains( "yarn" ) ) {
        Map yarnServiceFactoryMap = new HashMap<String, String>();
        yarnServiceFactoryMap.put( "shim", hadoopShim.getShimIdentifier().getId() );
        yarnServiceFactoryMap.put( "service", "yarn" );

        // Load EE Yarn service factory via reflection
        logger.debug( "Loading EE Yarn service factory via reflection" );
        NamedClusterServiceFactory<?> yarnServiceFactory =
          EEServiceReflectionLoader.loadEEYarnServiceFactory( hadoopFileSystemLocator );

        if ( yarnServiceFactory != null ) {
          try {
            namedClusterServiceLocator.factoryAdded( yarnServiceFactory, yarnServiceFactoryMap );
            logger.debug(
              "Successfully registered EE Yarn service factory: " + yarnServiceFactory.getClass().getSimpleName() );
          } catch ( Exception e ) {
            logger.warn( "Failed to register EE Yarn service factory: " + yarnServiceFactory.getClass().getName(), e );
          }
        } else {
          logger.debug( "EE Yarn service factory not available - continuing in CE mode" );
        }
      } else {
        logger.debug( "No Yarn services defined." );
      }

      //////////////////////////////////////////////////////////////////////////////////
      /// Bootstrap the run time tests
      //////////////////////////////////////////////////////////////////////////////////
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
    } catch ( ConfigurationException | ClassNotFoundException | IllegalAccessException | InstantiationException |
              IOException e ) {
      logger.error(
        "There was an error during the Pentaho Big Data Plugin bootstrap process. Some Big Data features may not be "
          + "available after startup.",
        e );
    }

    logger.debug( "Finished Pentaho Big Data Plugin bootstrap process." );

  }

  @Override
  public void onEnvironmentShutdown() {
    // No action needed on exit
  }
}
