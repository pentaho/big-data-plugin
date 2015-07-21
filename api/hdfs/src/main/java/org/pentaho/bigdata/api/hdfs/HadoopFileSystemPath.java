package org.pentaho.bigdata.api.hdfs;

import java.net.URI;

/**
 * Created by bryan on 5/27/15.
 */
public interface HadoopFileSystemPath {
  String getPath();
  String getName();
  String toString();
  URI toUri();

  HadoopFileSystemPath resolve( HadoopFileSystemPath child );
  HadoopFileSystemPath resolve( String child );

  HadoopFileSystemPath getParent();
}
