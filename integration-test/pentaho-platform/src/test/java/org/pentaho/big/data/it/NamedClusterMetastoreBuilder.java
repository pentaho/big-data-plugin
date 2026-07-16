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

package org.pentaho.big.data.it;

import org.pentaho.big.data.impl.cluster.NamedClusterImpl;
import org.pentaho.big.data.impl.cluster.NamedClusterManager;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.encryption.TwoWayPasswordEncoderPluginType;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.metastore.stores.xml.XmlMetaStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Generates the test named cluster (plus its {@code *-site.xml} files) into an on-disk PDI metastore that
 * is later assembled into the PDI container at {@code ~/.pentaho/metastore}. The named cluster points at
 * the Hadoop and HBase container aliases on the shared docker network, so the transformations can resolve
 * HDFS / HBase through the {@code apachevanilla} shim.
 * <p>
 * System properties:
 * <ul>
 *   <li>{@code bigdata.it.metastore-dir} (required) - output directory (becomes {@code ~/.pentaho/metastore}).</li>
 *   <li>{@code bigdata.it.named-cluster} (required) - named cluster name.</li>
 *   <li>{@code bigdata.it.hadoop-host} / {@code bigdata.it.hdfs-namenode-port}.</li>
 *   <li>{@code bigdata.it.hbase-host} / {@code bigdata.it.hbase-zookeeper-port}.</li>
 * </ul>
 */
public final class NamedClusterMetastoreBuilder {

  private NamedClusterMetastoreBuilder() {
  }

  public static void main( String[] args ) throws Exception {
    Path outputDir = Path.of( required( "bigdata.it.metastore-dir" ) ).toAbsolutePath().normalize();
    String clusterName = required( "bigdata.it.named-cluster" );
    String hadoopHost = required( "bigdata.it.hadoop-host" );
    String hdfsPort = required( "bigdata.it.hdfs-namenode-port" );
    String hbaseHost = required( "bigdata.it.hbase-host" );
    String zkPort = required( "bigdata.it.hbase-zookeeper-port" );

    PluginRegistry.addPluginType( TwoWayPasswordEncoderPluginType.getInstance() );
    PluginRegistry.init( false );
    Encr.init( "Kettle" );

    // XmlMetaStore(root) stores content under <root>/metastore; build in a temp root, then copy the
    // inner "metastore" folder content into the mount directory so it lands directly under ~/.pentaho/metastore.
    Path tempRoot = Files.createTempDirectory( "bigdata-it-metastore-" + UUID.randomUUID() );
    XmlMetaStore metaStore = new XmlMetaStore( tempRoot.toString() );

    NamedClusterImpl namedCluster = new NamedClusterImpl();
    namedCluster.setName( clusterName );
    namedCluster.setStorageScheme( "hdfs" );
    namedCluster.setHdfsHost( hadoopHost );
    namedCluster.setHdfsPort( hdfsPort );
    namedCluster.setZooKeeperHost( hbaseHost );
    namedCluster.setZooKeeperPort( zkPort );
    namedCluster.addSiteFile( "core-site.xml", coreSite( hadoopHost, hdfsPort ) );
    namedCluster.addSiteFile( "hdfs-site.xml", hdfsSite() );
    namedCluster.addSiteFile( "hbase-site.xml", hbaseSite( hbaseHost, zkPort ) );

    NamedClusterService namedClusterService = new NamedClusterManager();
    namedClusterService.create( namedCluster, metaStore );

    Path generated = tempRoot.resolve( "metastore" );
    if ( !Files.isDirectory( generated ) ) {
      throw new IOException( "Expected metastore content at " + generated + " but it was not created" );
    }
    Files.createDirectories( outputDir );
    copyDirectory( generated, outputDir );
    deleteRecursively( tempRoot );

    System.out.println( "[INFO] Named cluster '" + clusterName + "' written to " + outputDir );
    System.out.println( "[INFO] HDFS=hdfs://" + hadoopHost + ":" + hdfsPort
      + " ZooKeeper=" + hbaseHost + ":" + zkPort );
  }

  private static String coreSite( String host, String port ) {
    return property( "fs.defaultFS", "hdfs://" + host + ":" + port );
  }

  private static String hdfsSite() {
    return property( "dfs.replication", "1" );
  }

  private static String hbaseSite( String host, String port ) {
    return configuration(
      propertyElement( "hbase.zookeeper.quorum", host )
        + propertyElement( "hbase.zookeeper.property.clientPort", port ) );
  }

  private static String property( String name, String value ) {
    return configuration( propertyElement( name, value ) );
  }

  private static String propertyElement( String name, String value ) {
    return "  <property>\n    <name>" + name + "</name>\n    <value>" + value + "</value>\n  </property>\n";
  }

  private static String configuration( String body ) {
    return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<configuration>\n" + body + "</configuration>\n";
  }

  private static void copyDirectory( Path source, Path target ) throws IOException {
    try ( Stream<Path> paths = Files.walk( source ) ) {
      paths.forEach( path -> {
        try {
          Path relative = source.relativize( path );
          Path destination = target.resolve( relative.toString() );
          if ( Files.isDirectory( path ) ) {
            Files.createDirectories( destination );
          } else {
            Files.createDirectories( destination.getParent() );
            Files.copy( path, destination, java.nio.file.StandardCopyOption.REPLACE_EXISTING );
          }
        } catch ( IOException e ) {
          throw new RuntimeException( "Failed to copy metastore file " + path, e );
        }
      } );
    }
  }

  private static void deleteRecursively( Path dir ) throws IOException {
    try ( Stream<Path> paths = Files.walk( dir ) ) {
      paths.sorted( Comparator.reverseOrder() ).forEach( p -> {
        try {
          Files.deleteIfExists( p );
        } catch ( IOException ignored ) {
          // best-effort
        }
      } );
    }
  }

  private static String required( String key ) {
    String value = System.getProperty( key );
    if ( value == null || value.isBlank() ) {
      throw new IllegalStateException( "Missing required system property: " + key );
    }
    return value.trim();
  }
}
