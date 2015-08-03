package org.pentaho.bigdata.api.hdfs;

import org.pentaho.big.data.api.cluster.NamedCluster;

import java.io.IOException;

/**
 * Created by bryan on 5/28/15.
 */
public interface HadoopFileSystemFactory {
  boolean canHandle( NamedCluster namedCluster );
  HadoopFileSystem create( NamedCluster namedCluster ) throws IOException;
}
