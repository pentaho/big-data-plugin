package org.pentaho.bigdata.api.hdfs;

/**
 * Created by bryan on 5/27/15.
 */
public interface HadoopFileStatus {
  long getLen();

  boolean isDir();

  long getModificationTime();

  HadoopFileSystemPath getPath();
}
