/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2020 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.big.data.impl.cluster;

import org.apache.commons.io.FileUtils;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.encryption.TwoWayPasswordEncoderPluginType;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.KettleLoggingEvent;
import org.pentaho.di.core.logging.KettleLoggingEventListener;
import org.pentaho.di.core.osgi.api.NamedClusterSiteFile;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.stores.xml.XmlMetaStore;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class NamedClusterMetastoreIT {
  private static final String HDFS_PREFIX = "hdfs";

  private VariableSpace variableSpace;
  private NamedClusterImpl namedCluster;

  private String namedClusterName;
  private String namedClusterHdfsHost;
  private String namedClusterHdfsPort;
  private String namedClusterHdfsUsername;
  private String namedClusterHdfsPassword;
  private String namedClusterJobTrackerPort;
  private String namedClusterJobTrackerHost;
  private String namedClusterZookeeperHost;
  private String namedClusterZookeeperPort;
  private String namedClusterOozieUrl;
  private String namedClusterStorageScheme;
  private String namedClusterKafkaBootstrapServers;
  private boolean isMapr;
  private IMetaStore metaStore;
  private StandardFileSystemManager fsm;
  private NamedClusterService namedClusterService;
  private String fileContents1;
  private String metastoreRootFolder;
  private KettleLoggingEventListener kettleLoggingEventListener;
  private LinkedList<KettleLoggingEvent> loggingEventList;

  @Before
  public void setup() throws Exception {
    PluginRegistry.addPluginType( TwoWayPasswordEncoderPluginType.getInstance() );
    PluginRegistry.init( false );

    loggingEventList = new LinkedList<>();
    kettleLoggingEventListener = new KettleLoggingEventListener() {
      @Override public void eventAdded( KettleLoggingEvent event ) {
        loggingEventList.add( event );
      }
    };
    KettleLogStore.init();
    KettleLogStore.getAppender().addLoggingEventListener( kettleLoggingEventListener );
    Encr.init( "Kettle" );
    fileContents1 =
      FileUtils.readFileToString( new File( getClass().getResource( "/core-site.xml" ).getFile() ), "UTF-8" );

    metastoreRootFolder = System.getProperty( "java.io.tmpdir" ) + File.separator + UUID.randomUUID();
    metaStore = new XmlMetaStore( metastoreRootFolder );
    variableSpace = new Variables();
    namedCluster = new NamedClusterImpl();
    namedCluster.shareVariablesWith( variableSpace );
    namedClusterName = "namedClusterName";
    namedClusterHdfsHost = "namedClusterHdfsHost";
    namedClusterHdfsPort = "12345";
    namedClusterHdfsUsername = "namedClusterHdfsUsername";
    namedClusterHdfsPassword = "namedClusterHdfsPassword";
    namedClusterJobTrackerHost = "namedClusterJobTrackerHost";
    namedClusterJobTrackerPort = "namedClusterJobTrackerPort";
    namedClusterZookeeperHost = "namedClusterZookeeperHost";
    namedClusterZookeeperPort = "namedClusterZookeeperPort";
    namedClusterOozieUrl = "namedClusterOozieUrl";
    namedClusterStorageScheme = "hdfs";
    namedClusterKafkaBootstrapServers = "kafkaBootstrapServers";
    isMapr = true;

    namedCluster.setName( namedClusterName );
    namedCluster.setHdfsHost( namedClusterHdfsHost );
    namedCluster.setHdfsPort( namedClusterHdfsPort );
    namedCluster.setHdfsUsername( namedClusterHdfsUsername );
    namedCluster.setHdfsPassword( namedCluster.encodePassword( namedClusterHdfsPassword ) );
    namedCluster.setJobTrackerHost( namedClusterJobTrackerHost );
    namedCluster.setJobTrackerPort( namedClusterJobTrackerPort );
    namedCluster.setZooKeeperHost( namedClusterZookeeperHost );
    namedCluster.setZooKeeperPort( namedClusterZookeeperPort );
    namedCluster.setOozieUrl( namedClusterOozieUrl );
    namedCluster.setMapr( isMapr );
    namedCluster.setStorageScheme( namedClusterStorageScheme );
    namedCluster.setKafkaBootstrapServers( namedClusterKafkaBootstrapServers );
    namedCluster.addSiteFile( "core-site.xml", fileContents1 );
    namedCluster.addSiteFile( "fileName2", "fileContents2" );

    namedClusterService = new NamedClusterManager( );
  }
  @Test
  public void testWriteAndRead() throws Exception {
    namedClusterService.create( namedCluster, metaStore );
    NamedCluster nc = namedClusterService.getNamedClusterByName( namedClusterName, metaStore );
    assertEquals( namedClusterName, nc.getName() );
    assertEquals( fileContents1, getSiteFileContents( nc, "core-site.xml" ) );
  }

  @Test
  public void testAutoEmbedSiteFiles() throws Exception {
    commonAutoEmbedSetupLogic();

    namedClusterService.create( namedCluster, metaStore );
    NamedCluster nc = namedClusterService.getNamedClusterByName( namedClusterName, metaStore );
    assertEquals( namedClusterName, nc.getName() );
    assertEquals( fileContents1, getSiteFileContents( nc, "core-site.xml" ) );

  }

  @Test
  public void testAutoEmbedWhenUpdateMetastoreAndRecoveryFails() throws Exception {
    commonAutoEmbedSetupLogic();
    NamedClusterService disabledNamedClusterService = new NamedClusterManager() {
      @Override public void update( NamedCluster namedCluster, IMetaStore metastore ) throws MetaStoreException {
        throw new MetaStoreException( "Something bad happened" );
      }
    };
    namedClusterService = disabledNamedClusterService;
    namedClusterService.create( namedCluster, metaStore );

    NamedCluster nc = namedClusterService.getNamedClusterByName( namedClusterName, metaStore );
    assertEquals( 4, loggingEventList.size() );
  }

  @Test
  public void testAutoEmbedWhenUpdateMetastoreFails() throws Exception {
    commonAutoEmbedSetupLogic();
    NamedClusterService disabledNamedClusterService = new NamedClusterManager() {
      private int counter;

      @Override public void update( NamedCluster namedCluster, IMetaStore metastore ) throws MetaStoreException {
        counter++;
        if ( counter == 1 ) {
          //Force the first update (when we try to add the site files) to fail
          throw new MetaStoreException( "Something bad happened" );
        } else {
          //Thereafter the recovery update works
          super.update( namedCluster, metastore );
        }
      }
    };
    namedClusterService = disabledNamedClusterService;
    namedClusterService
      .create( namedCluster, metaStore ); //Create the namedCluster (without site files) in the metastore

    NamedCluster nc = namedClusterService.getNamedClusterByName( namedClusterName, metaStore );

    assertEquals( 2, loggingEventList.size() );
    assertEquals( namedClusterName, nc.getName() );
    assert ( nc.getSiteFiles().isEmpty() );
  }

  private void commonAutoEmbedSetupLogic() throws IOException {
    namedCluster.setSiteFiles( new ArrayList<NamedClusterSiteFile>() ); //No site files in named cluster
    File destFile = new File(
      metastoreRootFolder + File.separator + "metastore" + File.separator + "pentaho" + File.separator + "NamedCluster"
        + File.separator + "Configs" + File.separator + "namedClusterName" + File.separator + "core-site.xml" );
    destFile.getParentFile().mkdirs();
    //Put a site file out on the metastore
    FileUtils.copyFile( new File( getClass().getResource( "/core-site.xml" ).getFile() ), destFile );
  }


  private String getSiteFileContents( NamedCluster nc, String siteFileName ) {
    NamedClusterSiteFile n = nc.getSiteFiles().stream().filter( sf -> sf.getSiteFileName().equals( siteFileName ) )
      .findFirst().orElse( null );
    return n == null ? null : n.getSiteFileContents();
  }
}
