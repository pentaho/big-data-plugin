/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
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
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.*;

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

  @Test
  public void testCorruptedFileWithList() throws Exception {
    NamedClusterImpl corruptedNamedCluster = new NamedClusterImpl( namedCluster );
    final String corruptedName = "corruptedNamedCluster";
    corruptedNamedCluster.setName( corruptedName );
    corruptedNamedCluster.addSiteFile( "core-site.xml", Character.toString( (char) 5 ) ); //Make the site file corrupt

    namedClusterService.create( namedCluster, metaStore ); //Write the good one ...
    namedClusterService.create( corruptedNamedCluster, metaStore ); //... and the bad one

    //We should not get an error when we try to get the cluster by name because it uses a tolerant list
    //The list must be tolerant or a good clusters will never be returned.
    assertNotNull( namedClusterService.getNamedClusterByName( namedClusterName, metaStore ) );
    assertNull( namedClusterService.getNamedClusterByName( corruptedName, metaStore ) );
    List<MetaStoreException> exceptionList = new ArrayList<MetaStoreException>();

    //Getting the list with a non-null exceptionList is tolerant of corrupt entries.
    List<NamedCluster> namedClusterList = namedClusterService.list( metaStore, exceptionList );
    //The list contains the good cluster only
    assertEquals( 1, namedClusterList.size() );
    assertEquals( namedCluster, namedClusterList.get( 0 ) );
    assertEquals( 1, exceptionList.size() );
    assert
      ( exceptionList.get( 0 ).getMessage().contains( "Could not load metaStore element '" + corruptedName + "'" ) );

    //Even if we didn't ask for the exception list, NamedClusters should still be tolerant, even if the metastore would
    // not be.
    namedClusterService.list( metaStore );
  }
}
