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

import org.pentaho.big.data.api.cluster.service.locator.impl.NamedClusterServiceLocatorImpl;
import org.pentaho.big.data.impl.cluster.tests.hdfs.GatewayListHomeDirectoryTest;
import org.pentaho.big.data.impl.cluster.tests.hdfs.GatewayListRootDirectoryTest;
import org.pentaho.big.data.impl.cluster.tests.hdfs.GatewayPingFileSystemEntryPoint;
import org.pentaho.big.data.impl.cluster.tests.hdfs.GatewayWriteToAndDeleteFromUsersHomeFolderTest;
import org.pentaho.big.data.impl.cluster.tests.kafka.KafkaConnectTest;
import org.pentaho.big.data.impl.cluster.tests.mr.GatewayPingJobTrackerTest;
import org.pentaho.big.data.impl.cluster.tests.oozie.GatewayPingOozieHostTest;
import org.pentaho.big.data.impl.cluster.tests.zookeeper.GatewayPingZookeeperEnsembleTest;
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
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.hadoop.shim.HadoopConfigurationLocator;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystemFactory;
import com.pentaho.big.data.bundles.impl.shim.hdfs.HadoopFileSystemFactoryImpl;
import org.pentaho.bigdata.api.hdfs.impl.HadoopFileSystemLocatorImpl;
import org.pentaho.hadoop.shim.api.ConfigurationException;
import org.pentaho.big.data.impl.vfs.hdfs.HDFSFileProvider;
import org.apache.commons.vfs2.FileSystemException;
import org.pentaho.hadoop.shim.common.CommonFormatShim;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.i18n.impl.BaseMessagesMessageGetterFactoryImpl;
import org.pentaho.runtime.test.impl.RuntimeTesterImpl;
import org.pentaho.runtime.test.network.impl.ConnectivityTestFactoryImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

@KettleLifecyclePlugin( id = "BigDataPlugin", name = "Big Data Plugin" )
public class BigDataPluginLifecycleListener implements KettleLifecycleListener {

  private LogChannel log = new LogChannel( this );

  @Override
  public void onEnvironmentInit() throws LifecycleException {
      log.logDebug( "Starting Pentaho Big Data Plugin kettle lifecycle listener." );
      try {
          //////////////////////////////////////////////////////////////////////////////////
          /// Bootstrapping the HDFS Services
          //////////////////////////////////////////////////////////////////////////////////
          // 1. Set up the hadoopFileSystemService (HadoopFileSystemLocator)
          HadoopConfigurationBootstrap hadoopConfigurationBootstrap = HadoopConfigurationBootstrap.getInstance();
          HadoopConfigurationLocator hadoopConfigurationProvider = null;
          hadoopConfigurationProvider = (HadoopConfigurationLocator) hadoopConfigurationBootstrap.getProvider();
          HadoopConfiguration hadoopConfiguration = hadoopConfigurationProvider.getActiveConfiguration();

          HadoopFileSystemFactory hadoopFileSystemFactory =
                  new HadoopFileSystemFactoryImpl( hadoopConfiguration.getHadoopShim(), hadoopConfiguration.getHadoopShim().getShimIdentifier() );
          List<HadoopFileSystemFactory> hadoopFileSystemFactoryList = new ArrayList<>();
          hadoopFileSystemFactoryList.add( hadoopFileSystemFactory );
          // TODO: Move the HadoopFileSystemLocatorImpl to a singleton. (NOTE: Might NOT be required anymore since
          //  the Bootstrap the run time tests were added to this listener.
          HadoopFileSystemLocatorImpl hadoopFileSystemLocator = new HadoopFileSystemLocatorImpl( hadoopFileSystemFactoryList );

          // 2. Set up the namedClusterService (NamedClusterService)
          // Not needed, moved to HDFSFileProvider constructor

          // 3. Set up the hdfsFileNameParser (HDFSFileNameParser)
          // Not needed, moved to HDFSFileProvider constructor

          // 4. Set up new HDFSFileProviders based on old big-data-plugin/impl/vfs/hdfs/src/main/resources/OSGI-INF/blueprint/blueprint.xml:
          // schema=hdfs
          HDFSFileProvider hdfsHDFSFileProvider = new HDFSFileProvider( hadoopFileSystemLocator, "hdfs", HDFSFileNameParser.getInstance() );
          // schema=maprfs
          HDFSFileProvider maprfsHDFSFileProvider = new HDFSFileProvider( hadoopFileSystemLocator, "maprfs", MapRFileNameParser.getInstance() );
          // schema=escalefs
          HDFSFileProvider escalefsHDFSFileProvider = new HDFSFileProvider( hadoopFileSystemLocator, "escalefs", MapRFileNameParser.getInstance() );
          // schema=wasb
          HDFSFileProvider wasbHDFSFileProvider = new HDFSFileProvider( hadoopFileSystemLocator, "wasb", AzureHdInsightsFileNameParser.getInstance() );
          // schema=wasbs
          HDFSFileProvider wasbsHDFSFileProvider = new HDFSFileProvider( hadoopFileSystemLocator, "wasbs", AzureHdInsightsFileNameParser.getInstance() );
          // schema=abfs
          HDFSFileProvider abfsHDFSFileProvider = new HDFSFileProvider( hadoopFileSystemLocator, "abfs", AzureHdInsightsFileNameParser.getInstance() );

          // 5. Set up a NamedClusterProvider for the "hc" schema
          NamedClusterProvider namedClusterProvider = new NamedClusterProvider( hadoopFileSystemLocator, "hc", HDFSFileNameParser.getInstance() );

          //////////////////////////////////////////////////////////////////////////////////
          /// Bootstrap the common format service factories
          //////////////////////////////////////////////////////////////////////////////////
          // 1. Initialize the NamedClusterServiceLocatorImpl
          NamedClusterServiceLocatorImpl namedClusterServiceLocator = NamedClusterServiceLocatorImpl.getInstance();
          // 2. Add the Format NamedClusterServiceFactory to the factory map
          CommonFormatShim commonFormatShim = new CommonFormatShim();
          FormatServiceFactory formatServiceFactory = new FormatServiceFactory( commonFormatShim );
          Map formatFactoryMap = new HashMap<String, String>();
          formatFactoryMap.put( "shim", hadoopConfiguration.getIdentifier() );
          formatFactoryMap.put( "service", "format" );
          // 3. Add the factory map to the NamedClusterServiceLocatorImpl
          namedClusterServiceLocator.factoryAdded( formatServiceFactory, formatFactoryMap );

          //////////////////////////////////////////////////////////////////////////////////
          /// Bootstrap the mapreduce service factories
          //////////////////////////////////////////////////////////////////////////////////
          //  2. Add the MapReduce NamedClusterServiceFactory to the factory map
//          ProcessGovernorImpl processGovernor = new ProcessGovernorImpl(
//                  Executors.newCachedThreadPool(),
//                  2000
//          );
          List<TransformationVisitorService> visitorServices = new ArrayList<>();
          MapReduceServiceFactoryImpl mapReduceServiceFactory = new MapReduceServiceFactoryImpl(
                  hadoopConfiguration.getHadoopShim(),
                  Executors.newCachedThreadPool(),
                  visitorServices
                  );
          Map mapReducefactoryMap = new HashMap<String, String>();
          mapReducefactoryMap.put( "shim", hadoopConfiguration.getIdentifier() );
          mapReducefactoryMap.put( "service", "mapreduce" );
          // 3. Add the factory map to the NamedClusterServiceLocatorImpl
          namedClusterServiceLocator.factoryAdded( mapReduceServiceFactory, mapReducefactoryMap );

          //////////////////////////////////////////////////////////////////////////////////
          /// Bootstrap the hadoop client service factories
          //////////////////////////////////////////////////////////////////////////////////
          //  2. Add the hadoop client NamedClusterServiceFactory to the factory map

          //////////////////////////////////////////////////////////////////////////////////
          /// Bootstrap the run time tests
          //////////////////////////////////////////////////////////////////////////////////
          RuntimeTester runtimeTester = RuntimeTesterImpl.getInstance();
          runtimeTester.addRuntimeTest( new GatewayPingFileSystemEntryPoint(  BaseMessagesMessageGetterFactoryImpl.getInstance(), new ConnectivityTestFactoryImpl() ) );
          runtimeTester.addRuntimeTest( new GatewayPingJobTrackerTest(  BaseMessagesMessageGetterFactoryImpl.getInstance(), new ConnectivityTestFactoryImpl() ) );
          runtimeTester.addRuntimeTest( new GatewayPingOozieHostTest(  BaseMessagesMessageGetterFactoryImpl.getInstance(), new ConnectivityTestFactoryImpl() ) );
          runtimeTester.addRuntimeTest( new GatewayPingZookeeperEnsembleTest(  BaseMessagesMessageGetterFactoryImpl.getInstance(), new ConnectivityTestFactoryImpl() ) );
          runtimeTester.addRuntimeTest( new GatewayListRootDirectoryTest( BaseMessagesMessageGetterFactoryImpl.getInstance(), new ConnectivityTestFactoryImpl(), hadoopFileSystemLocator ) );
          runtimeTester.addRuntimeTest( new GatewayListHomeDirectoryTest( BaseMessagesMessageGetterFactoryImpl.getInstance(), new ConnectivityTestFactoryImpl(), hadoopFileSystemLocator ) );
          runtimeTester.addRuntimeTest( new GatewayWriteToAndDeleteFromUsersHomeFolderTest( BaseMessagesMessageGetterFactoryImpl.getInstance(), hadoopFileSystemLocator ) );
          runtimeTester.addRuntimeTest( new KafkaConnectTest( BaseMessagesMessageGetterFactoryImpl.getInstance(), namedClusterServiceLocator ) );
      } catch (ConfigurationException | FileSystemException e) {
          throw new RuntimeException(e);
      }

  }

  @Override
  public void onEnvironmentShutdown() {
      // No action needed on exit
  }
}
