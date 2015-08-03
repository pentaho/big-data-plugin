package org.pentaho.bigdata.api.hdfs;

import org.pentaho.big.data.api.cluster.NamedCluster;

/**
 * Created by bryan on 5/22/15.
 */
public interface HadoopFileSystemLocator {
  HadoopFileSystem getHadoopFilesystem( NamedCluster namedCluster );
}