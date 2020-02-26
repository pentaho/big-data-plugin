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

import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.encryption.TwoWayPasswordEncoderPluginType;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.stores.xml.XmlMetaStore;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

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

  @Before
  public void setup() throws Exception {
    PluginRegistry.addPluginType( TwoWayPasswordEncoderPluginType.getInstance() );
    PluginRegistry.init( false );
    Encr.init( "Kettle" );
    String fileContents1 = fileToString( new File( getClass().getResource( "/core-site.xml" ).getFile() ).toString() );

    metaStore = new XmlMetaStore();
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
  public void testDummy() throws Exception {
    namedClusterService.create( namedCluster, metaStore );
    NamedCluster nc = namedClusterService.getNamedClusterByName( namedClusterName, metaStore );
    assertEquals( namedClusterName, nc.getName() );
  }

  private static String fileToString( String filePath ) {
    StringBuilder sb = new StringBuilder();

    try ( Stream<String> stream = Files.lines( Paths.get( filePath ), StandardCharsets.UTF_8 ) ) {
      stream.forEach( s -> sb.append( s ).append( "\n" ) );
    } catch ( IOException e ) {
      e.printStackTrace();
    }

    return sb.toString();
  }
}
