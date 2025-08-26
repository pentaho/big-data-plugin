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
import com.pentaho.big.data.ee.secure.impersonation.service.AuthRequestToUGIMappingService;
import com.pentaho.hadoop.shim.ImpersonatingHadoopClientServicesFactory;
import com.pentaho.hadoop.shim.hbase.security.impersonation.HBaseImpersonationServiceFactory;
import com.pentaho.hadoop.shim.hbase.security.knox.HBaseKnoxServiceFactory;
import com.pentaho.hadoop.shim.hdfs.security.impersonation.ImpersonatingHadoopFileSystemFactoryImpl;
import com.pentaho.hadoop.shim.hdfs.security.knox.KnoxHadoopFileSystemFactoryImpl;
import com.pentaho.hadoop.shim.hive.security.impersonation.*;
import com.pentaho.hadoop.shim.mapreduce.security.impersonation.MapReduceImpersonationServiceFactory;
import com.pentaho.hadoop.shim.mapreduce.security.impersonation.knox.KnoxMapReduceServiceFactory;
import com.pentaho.yarn.impl.shim.YarnServiceFactoryImpl;
import org.pentaho.authentication.mapper.api.AuthenticationMappingManager;
import org.pentaho.authentication.mapper.impl.AuthenticationMappingManagerImpl;
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
import org.pentaho.big.data.impl.vfs.hdfs.AzureHdInsightsFileNameParser;
import org.pentaho.big.data.impl.vfs.hdfs.HDFSFileNameParser;
import org.pentaho.big.data.impl.vfs.hdfs.MapRFileNameParser;
import org.pentaho.big.data.impl.vfs.hdfs.nc.NamedClusterProvider;
import org.pentaho.di.core.annotations.KettleLifecyclePlugin;
import org.pentaho.di.core.lifecycle.KettleLifecycleListener;
import org.pentaho.di.core.lifecycle.LifecycleException;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import com.pentaho.big.data.ee.secure.impersonation.service.impersonation.SimpleMapping;
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
              SimpleMapping simpleMapping = new SimpleMapping();
              AuthRequestToUGIMappingService authRequestToUGIMappingService =
                      new AuthRequestToUGIMappingService(
                              hadoopShim,
                              simpleMapping
                      );
              authenticationMappingManager = new AuthenticationMappingManagerImpl(
                                authRequestToUGIMappingService
                      );
          } else {
              logger.debug( "No authentication manager service defined." );
          }
          //////////////////////////////////////////////////////////////////////////////////
          /// Bootstrapping the HDFS Services
          //////////////////////////////////////////////////////////////////////////////////
          // 1. Set up the hadoopFileSystemService (HadoopFileSystemLocator)
          HadoopConfiguration hadoopConfiguration = hadoopConfigurationProvider.getActiveConfiguration();
          HadoopShim hadoopShim = hadoopConfiguration.getHadoopShim();
          List<String> shimAvailableServices = hadoopShim.getAvailableServices();

          //////////////////////////////////////////////////////////////////////////////////
          /// Bootstrapping the authentication manager service
          //////////////////////////////////////////////////////////////////////////////////
          logger.debug( "Bootstrapping the authentication manager service." );
          AuthenticationMappingManager authenticationMappingManager = null;
          if ( shimAvailableServices.contains( "auth_manager" ) ) {
              SimpleMapping simpleMapping = new SimpleMapping();
              AuthRequestToUGIMappingService authRequestToUGIMappingService =
                      new AuthRequestToUGIMappingService(
                              hadoopShim,
                              simpleMapping
                      );
              authenticationMappingManager = new AuthenticationMappingManagerImpl(
                                authRequestToUGIMappingService
                      );
          } else {
              logger.debug( "No authentication manager service defined." );
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
                  HadoopFileSystemFactory hadoopFileSystemFactory = new HadoopFileSystemFactoryImpl(hadoopShim, hadoopShim.getShimIdentifier());
                  hadoopFileSystemFactoryList.add(hadoopFileSystemFactory);
              }
              if ( availableHdfsOptions.contains( "hdfs_impersonation" ) ) {
                  logger.debug( "Adding 'hdfs_impersonation' factory." );
                  ImpersonatingHadoopFileSystemFactoryImpl impersonatingHadoopFileSystemFactory =
                          new ImpersonatingHadoopFileSystemFactoryImpl(
                                  hadoopShim,
                                  hadoopShim.getShimIdentifier(),
                                  authenticationMappingManager
                          );
                  hadoopFileSystemFactoryList.add( impersonatingHadoopFileSystemFactory );
              }
              if ( availableHdfsOptions.contains( "hdfs_knox" ) ) {
                  logger.debug( "Adding 'hdfs_knox' factory." );
                  KnoxHadoopFileSystemFactoryImpl knoxHadoopFileSystemFactory =
                          new KnoxHadoopFileSystemFactoryImpl(
                                  hadoopShim,
                                  hadoopShim.getShimIdentifier()
                          );
                  hadoopFileSystemFactoryList.add( knoxHadoopFileSystemFactory );
              }
              hadoopFileSystemLocator = HadoopFileSystemLocatorImpl.getInstance();
              hadoopFileSystemLocator.setHadoopFileSystemFactories( hadoopFileSystemFactoryList );

              if ( availableHdfsSchemas.contains( "hdfs" ) ) {
                  logger.debug( "Adding 'hdfs' schema.'" );
                  HDFSFileProvider hdfsHDFSFileProvider = new HDFSFileProvider(hadoopFileSystemLocator, "hdfs", HDFSFileNameParser.getInstance());
              }
              // schema=maprfs
              if ( availableHdfsSchemas.contains( "maprfs" ) ) {
                  logger.debug( "Adding 'maprfs' schema.'" );
                  HDFSFileProvider maprfsHDFSFileProvider = new HDFSFileProvider(hadoopFileSystemLocator, "maprfs", MapRFileNameParser.getInstance());
              }
              // schema=escalefs
              if ( availableHdfsSchemas.contains( "escalefs" ) ) {
                  logger.debug( "Adding 'escalefs' schema.'" );
                  HDFSFileProvider escalefsHDFSFileProvider = new HDFSFileProvider(hadoopFileSystemLocator, "escalefs", MapRFileNameParser.getInstance());
              }
              // schema=wasb
              if ( availableHdfsSchemas.contains( "wasb" ) ) {
                  logger.debug( "Adding 'wasb' schema.'" );
                  HDFSFileProvider wasbHDFSFileProvider = new HDFSFileProvider(hadoopFileSystemLocator, "wasb", AzureHdInsightsFileNameParser.getInstance());
              }
              // schema=wasbs
              if ( availableHdfsSchemas.contains( "wasbs" ) ) {
                  logger.debug( "Adding 'wasbs' schema.'" );
                  HDFSFileProvider wasbsHDFSFileProvider = new HDFSFileProvider(hadoopFileSystemLocator, "wasbs", AzureHdInsightsFileNameParser.getInstance());
              }
              // schema=abfs
              if ( availableHdfsSchemas.contains( "abfs" ) ) {
                  logger.debug( "Adding 'abfs' schema.'" );
                  HDFSFileProvider abfsHDFSFileProvider = new HDFSFileProvider(hadoopFileSystemLocator, "abfs", AzureHdInsightsFileNameParser.getInstance());
              }
              // schema=hc
              if ( availableHdfsSchemas.contains( "hc" ) ) {
                  logger.debug( "Adding 'hc' schema.'" );
                  NamedClusterProvider namedClusterProvider = new NamedClusterProvider(hadoopFileSystemLocator, "hc", HDFSFileNameParser.getInstance());
                  org.pentaho.big.data.impl.browse.NamedClusterProvider namedClusterProvider2 = new org.pentaho.big.data.impl.browse.NamedClusterProvider();
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
              FormatServiceFactory formatServiceFactory = new FormatServiceFactory(commonFormatShim);
              Map formatFactoryMap = new HashMap<String, String>();
              formatFactoryMap.put("shim", hadoopShim.getShimIdentifier().getId());
              formatFactoryMap.put("service", "format");
              namedClusterServiceLocator.factoryAdded(formatServiceFactory, formatFactoryMap);
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
              mapReducefactoryMap.put("shim", hadoopShim.getShimIdentifier().getId());
              mapReducefactoryMap.put("service", "mapreduce");
              if ( availableMapreduceOptions.contains( "mapreduce" ) ) {
                  logger.debug( "Adding 'mapreduce' factory." );
                  MapReduceServiceFactoryImpl mapReduceServiceFactory = new MapReduceServiceFactoryImpl(
                          hadoopShim,
                          Executors.newCachedThreadPool(),
                          visitorServices
                  );
                  Map mapReducefactoryMap = new HashMap<String, String>();
          mapReducefactoryMap.put( "shim", hadoopShim.getShimIdentifier().getId() );
          mapReducefactoryMap.put( "service", "shimservices" );
          // 3. Add the factory map to the NamedClusterServiceLocatorImpl
          namedClusterServiceLocator.factoryAdded( mapReduceServiceFactory, mapReducefactoryMap );}
              if ( availableMapreduceOptions.contains( "mapreduce_impersonation" ) ) {
                  logger.debug( "Adding 'mapreduce_impersonation' factory." );
                  MapReduceImpersonationServiceFactory mapReduceImpersonationServiceFactory =
                          new MapReduceImpersonationServiceFactory(
                                  hadoopShim,
                                  Executors.newCachedThreadPool(),
                                  authenticationMappingManager,
                                  visitorServices
                          );
                  namedClusterServiceLocator.factoryAdded( mapReduceImpersonationServiceFactory, mapReducefactoryMap );
              }
              if ( availableMapreduceOptions.contains( "mapreduce_knox" ) ) {
                  logger.debug( "Adding 'mapreduce_knox' factory." );
                  KnoxMapReduceServiceFactory knoxMapReduceServiceFactory =
                          new KnoxMapReduceServiceFactory(
                                  hadoopShim,
                                  visitorServices
                          );
                  namedClusterServiceLocator.factoryAdded( knoxMapReduceServiceFactory, mapReducefactoryMap );
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
              hadoopClientFactoryMap.put("shim", hadoopShim.getShimIdentifier().getId());
              hadoopClientFactoryMap.put("service", "shimservices");
              if ( availableSqoopOptions.contains( "sqoop" ) ) {
                  logger.debug( "Adding 'sqoop' factory." );
                  HadoopClientServicesFactory hadoopClientServicesFactory = new HadoopClientServicesFactory(hadoopShim);
                  namedClusterServiceLocator.factoryAdded( hadoopClientServicesFactory, hadoopClientFactoryMap );
              }
              if ( availableSqoopOptions.contains( "sqoop_impersonation" ) ) {
                  logger.debug( "Adding 'sqoop_impersonation' factory." );
                  ImpersonatingHadoopClientServicesFactory impersonatingHadoopClientServicesFactory = new ImpersonatingHadoopClientServicesFactory(
                          hadoopShim,
                          authenticationMappingManager
                  );
                  namedClusterServiceLocator.factoryAdded( impersonatingHadoopClientServicesFactory, hadoopClientFactoryMap );
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
                      pentaho_jdbc_lazydrivers_num);
              List<String> availableHiveDrivers = hadoopShim.getAvailableHiveDrivers();
              // Hive
              if ( availableHiveDrivers.contains( "hive" ) ) {
                  logger.debug( "Adding 'hive' driver." );
                  HiveDriver hiveDriver = new HiveDriver(
                          jdbcUrlParser,
                          "org.apache.hive.jdbc.HiveDriver",
                          hadoopShim.getShimIdentifier().getId());
                  driverLocator.registerDriver( hiveDriver );
              }
              // Impala
              if ( availableHiveDrivers.contains( "impala" ) ) {
                  logger.debug( "Adding 'impala' driver." );
                  ImpalaDriver impalaDriver = new ImpalaDriver(
                          jdbcUrlParser,
                          "com.cloudera.impala.jdbc41.Driver",
                          hadoopShim.getShimIdentifier().getId());
                  driverLocator.registerDriver( impalaDriver );
              }
              // ImpalaSimba
              if ( availableHiveDrivers.contains( "impala_simba" ) ) {
                  logger.debug( "Adding 'impala_simba' driver." );
                  ImpalaSimbaDriver impalaSimbaDriver = new ImpalaSimbaDriver(
                          jdbcUrlParser,
                          "com.cloudera.impala.jdbc41.Driver",
                          hadoopShim.getShimIdentifier().getId());
                  driverLocator.registerDriver( impalaSimbaDriver );
              }
              // SparkSimba
              if ( availableHiveDrivers.contains( "spark_simba" ) ) {
                  logger.debug( "Adding 'spark_simba' driver." );
                  SparkSimbaDriver sparkSimbaDriver = new SparkSimbaDriver(
                          jdbcUrlParser,
                          "org.apache.hive.jdbc.HiveDriver",
                          hadoopShim.getShimIdentifier().getId());
                  driverLocator.registerDriver( sparkSimbaDriver );
              }
              // Impersonated services
              // Hive
              if ( availableHiveDrivers.contains( "hive_impersonation" ) ) {
                  logger.debug( "Adding 'hive_impersonation' driver." );
                  ImpersonatingHiveDriver impersonatingHiveDriver = new ImpersonatingHiveDriver(
                          "org.apache.hive.jdbc.HiveDriver",
                          hadoopShim.getShimIdentifier().getId(),
                          jdbcUrlParser,
                          authenticationMappingManager
                  );
                  driverLocator.registerDriver( impersonatingHiveDriver );
              }
              // Impala
              if ( availableHiveDrivers.contains( "impala_impersonation" ) ) {
                  logger.debug( "Adding 'impala_impersonation' driver." );
                  ImpersonatingImpalaDriver impersonatingImpalaDriver = new ImpersonatingImpalaDriver(
                          "com.cloudera.impala.jdbc41.Driver",
                          hadoopShim.getShimIdentifier().getId(),
                          jdbcUrlParser,
                          authenticationMappingManager
                  );
                  driverLocator.registerDriver( impersonatingImpalaDriver );
              }
              // Simba
              if ( availableHiveDrivers.contains( "simba_impersonation" ) ) {
                  logger.debug( "Adding 'simba_impersonation' driver." );
                  ImpersonatingHiveSimbaDriver impersonatingHiveSimbaDriver = new ImpersonatingHiveSimbaDriver(
                          "com.cloudera.impala.jdbc41.Driver",
                          hadoopShim.getShimIdentifier().getId(),
                          jdbcUrlParser,
                          authenticationMappingManager
                  );
                  driverLocator.registerDriver( impersonatingHiveSimbaDriver );
              }
              // ImpalaSimba
              if ( availableHiveDrivers.contains( "impala_simba_impersonation" ) ) {
                  logger.debug( "Adding 'impala_simba_impersonation' driver." );
                  ImpersonatingImpalaSimbaDriver impersonatingImpalaSimbaDriver = new ImpersonatingImpalaSimbaDriver(
                          "com.cloudera.impala.jdbc41.Driver",
                          hadoopShim.getShimIdentifier().getId(),
                          jdbcUrlParser,
                          authenticationMappingManager
                  );
                  driverLocator.registerDriver( impersonatingImpalaSimbaDriver );
              }
              // SparkSimba
              if ( availableHiveDrivers.contains( "spark_simba_impersonation" ) ) {
                  logger.debug( "Adding 'spark_simba_impersonation' driver." );
                  ImpersonatingSparkSqlSimbaDriver impersonatingSparkSqlSimbaDriver = new ImpersonatingSparkSqlSimbaDriver(
                          "org.apache.hive.jdbc.HiveDriver",
                          hadoopShim.getShimIdentifier().getId(),
                          jdbcUrlParser,
                          authenticationMappingManager
                  );
                  driverLocator.registerDriver( impersonatingSparkSqlSimbaDriver );
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
              hBaseServiceFactoryMap.put("shim", hadoopShim.getShimIdentifier().getId());
              hBaseServiceFactoryMap.put("service", "hbase");

              if ( availableHbaseOptions.contains( "hbase" ) ) {
                  logger.debug( "Adding 'hbase' factory." );
                  HBaseServiceFactory hBaseServiceFactory = new HBaseServiceFactory(hBaseShim);
                  namedClusterServiceLocator.factoryAdded(hBaseServiceFactory, hBaseServiceFactoryMap);
              }
              if ( availableHbaseOptions.contains( "hbase_impersonation" ) ) {
                  logger.debug( "Adding 'hbase_impersonation' factory." );
                  HBaseImpersonationServiceFactory hBaseImpersonationServiceFactory =
                          new HBaseImpersonationServiceFactory(
                                  hBaseShim,
                                  authenticationMappingManager
                          );
                  namedClusterServiceLocator.factoryAdded( hBaseImpersonationServiceFactory, hBaseServiceFactoryMap );
              }
              if ( availableHbaseOptions.contains( "hbase_knox" ) ) {
                  logger.debug( "Adding 'hbase_knox' factory." );
                  HBaseKnoxServiceFactory hBaseKnoxServiceFactory =
                          new HBaseKnoxServiceFactory(
                                  hBaseShim
                          );
                  namedClusterServiceLocator.factoryAdded( hBaseKnoxServiceFactory, hBaseServiceFactoryMap );
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
              yarnServiceFactoryMap.put("shim", hadoopShim.getShimIdentifier().getId());
              yarnServiceFactoryMap.put("service", "yarn");
              YarnServiceFactoryImpl yarnServiceFactory = new YarnServiceFactoryImpl(
                      hadoopFileSystemLocator,
                      authenticationMappingManager
              );
              namedClusterServiceLocator.factoryAdded( yarnServiceFactory, yarnServiceFactoryMap );
          } else {
              logger.debug( "No Yarn services defined." );
          }

          //////////////////////////////////////////////////////////////////////////////////
          /// Bootstrap the run time tests
          //////////////////////////////////////////////////////////////////////////////////
          logger.debug( "Bootstrap the run time tests." );
          RuntimeTester runtimeTester = RuntimeTesterImpl.getInstance();
          runtimeTester.addRuntimeTest( new GatewayPingFileSystemEntryPoint(  BaseMessagesMessageGetterFactoryImpl.getInstance(), new ConnectivityTestFactoryImpl() ) );
          runtimeTester.addRuntimeTest( new GatewayPingJobTrackerTest(  BaseMessagesMessageGetterFactoryImpl.getInstance(), new ConnectivityTestFactoryImpl() ) );
          runtimeTester.addRuntimeTest( new GatewayPingOozieHostTest(  BaseMessagesMessageGetterFactoryImpl.getInstance(), new ConnectivityTestFactoryImpl() ) );
          runtimeTester.addRuntimeTest( new GatewayPingZookeeperEnsembleTest(  BaseMessagesMessageGetterFactoryImpl.getInstance(), new ConnectivityTestFactoryImpl() ) );
          runtimeTester.addRuntimeTest( new GatewayListRootDirectoryTest( BaseMessagesMessageGetterFactoryImpl.getInstance(), new ConnectivityTestFactoryImpl(), hadoopFileSystemLocator ) );
          runtimeTester.addRuntimeTest( new GatewayListHomeDirectoryTest( BaseMessagesMessageGetterFactoryImpl.getInstance(), new ConnectivityTestFactoryImpl(), hadoopFileSystemLocator ) );
          runtimeTester.addRuntimeTest( new GatewayWriteToAndDeleteFromUsersHomeFolderTest( BaseMessagesMessageGetterFactoryImpl.getInstance(), hadoopFileSystemLocator ) );
          runtimeTester.addRuntimeTest( new KafkaConnectTest( BaseMessagesMessageGetterFactoryImpl.getInstance(), namedClusterServiceLocator ) );
      } catch (ConfigurationException | ClassNotFoundException | IllegalAccessException | InstantiationException | IOException e) {
          logger.error( "There was an error during the Pentaho Big Data Plugin bootstrap process. Some Big Data features may not be available after startup.", e );
      } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
      } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
      } catch (InstantiationException e) {
          throw new RuntimeException(e);
      } catch (IOException e) {
          throw new RuntimeException(e);
      }

      logger.debug( "Finished Pentaho Big Data Plugin bootstrap process." );

      logger.debug( "Finished Pentaho Big Data Plugin bootstrap process." );

      logger.debug( "Finished Pentaho Big Data Plugin bootstrap process." );

  }

  @Override
  public void onEnvironmentShutdown() {
      // No action needed on exit
  }
}
