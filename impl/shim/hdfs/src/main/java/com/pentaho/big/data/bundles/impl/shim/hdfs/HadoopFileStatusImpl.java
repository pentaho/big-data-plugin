package com.pentaho.big.data.bundles.impl.shim.hdfs;

import org.apache.hadoop.fs.FileStatus;
import org.pentaho.bigdata.api.hdfs.HadoopFileStatus;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemPath;

/**
 * Created by bryan on 5/28/15.
 */
public class HadoopFileStatusImpl implements HadoopFileStatus {
  private final FileStatus fileStatus;

  public HadoopFileStatusImpl( FileStatus fileStatus ) {
    this.fileStatus = fileStatus;
  }

  @Override
  public long getLen() {
    return fileStatus.getLen();
  }

  @Override
  public boolean isDir() {
    return fileStatus.isDir();
  }

  @Override
  public long getModificationTime() {
    return fileStatus.getModificationTime();
  }

  @Override
  public HadoopFileSystemPath getPath() {
    return new HadoopFileSystemPathImpl( fileStatus.getPath() );
  }
}
