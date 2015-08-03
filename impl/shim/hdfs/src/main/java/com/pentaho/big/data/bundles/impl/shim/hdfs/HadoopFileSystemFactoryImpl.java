package com.pentaho.big.data.bundles.impl.shim.hdfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystem;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemFactory;
import org.pentaho.di.core.Const;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by bryan on 5/28/15.
 */
public class HadoopFileSystemFactoryImpl implements HadoopFileSystemFactory {
  public static final String SHIM_IDENTIFIER = "shim.identifier";
  public static final String HDFS = "hdfs";
  private static final Logger LOGGER = LoggerFactory.getLogger( HadoopFileSystemFactoryImpl.class );
  private final boolean isActiveConfiguration;
  private final HadoopConfiguration hadoopConfiguration;

  public HadoopFileSystemFactoryImpl( boolean isActiveConfiguration, HadoopConfiguration hadoopConfiguration ) {
    this.isActiveConfiguration = isActiveConfiguration;
    this.hadoopConfiguration = hadoopConfiguration;
  }

  @Override public boolean canHandle( NamedCluster namedCluster ) {
    String shimIdentifier = null; // TODO: Specify shim
    return ( shimIdentifier == null && isActiveConfiguration ) || hadoopConfiguration.getIdentifier()
      .equals( shimIdentifier );
  }

  @Override public HadoopFileSystem create( NamedCluster namedCluster ) throws IOException {
    HadoopShim hadoopShim = hadoopConfiguration.getHadoopShim();
    Configuration configuration = hadoopShim.createConfiguration();
    String fsDefault;
    //TODO: AUTH
    if ( namedCluster.isMapr() ) {
      fsDefault = "mapr:///";
    } else {
      fsDefault = "hdfs://" + namedCluster.getHdfsHost();
      if ( !Const.isEmpty( namedCluster.getHdfsPort() ) ) {
        fsDefault = fsDefault + ":" + namedCluster.getHdfsPort();
      }
    }
    configuration.set( HadoopFileSystem.FS_DEFAULT_NAME, fsDefault );
    FileSystem fileSystem = (FileSystem) hadoopShim.getFileSystem( configuration ).getDelegate();
    if ( fileSystem instanceof LocalFileSystem ) {
      throw new IOException( "Got a local filesystem, was expecting an hdfs connection" );
    }
    return new HadoopFileSystemImpl( fileSystem );
  }
}
