package org.pentaho.bigdata.api.hdfs.impl;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystem;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemFactory;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemLocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Created by bryan on 6/4/15.
 */
public class HadoopFileSystemLocatorImpl implements HadoopFileSystemLocator {
  private static final Logger LOGGER = LoggerFactory.getLogger( HadoopFileSystemLocatorImpl.class );
  private final List<HadoopFileSystemFactory> hadoopFileSystemFactories;

  public HadoopFileSystemLocatorImpl( List<HadoopFileSystemFactory> hadoopFileSystemFactories ) {
    this.hadoopFileSystemFactories = hadoopFileSystemFactories;
  }

  @Override public HadoopFileSystem getHadoopFilesystem( NamedCluster namedCluster ) {
    for ( HadoopFileSystemFactory hadoopFileSystemFactory : hadoopFileSystemFactories ) {
      if ( hadoopFileSystemFactory.canHandle( namedCluster ) ) {
        try {
          return hadoopFileSystemFactory.create( namedCluster );
        } catch ( IOException e ) {
          LOGGER.warn( "Unable to create hdfs service with " + hadoopFileSystemFactory + " for " + namedCluster, e );
        }
      }
    }
    return null;
  }
}
