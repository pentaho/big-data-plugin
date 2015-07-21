package org.pentaho.bigdata.api.hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface HadoopFileSystem {
  public static final String FS_DEFAULT_NAME = "fs.default.name";

  OutputStream append( HadoopFileSystemPath path ) throws IOException;

  OutputStream create( HadoopFileSystemPath path ) throws IOException;

  boolean delete( HadoopFileSystemPath path, boolean arg1 ) throws IOException;

  HadoopFileStatus getFileStatus( HadoopFileSystemPath path ) throws IOException;

  boolean mkdirs( HadoopFileSystemPath path ) throws IOException;

  InputStream open( HadoopFileSystemPath path ) throws IOException;

  boolean rename( HadoopFileSystemPath path, HadoopFileSystemPath path2 ) throws IOException;

  void setTimes( HadoopFileSystemPath path, long mtime, long atime ) throws IOException;

  HadoopFileStatus[] listStatus( HadoopFileSystemPath path ) throws IOException;

  HadoopFileSystemPath getPath( String path );

  HadoopFileSystemPath makeQualified( HadoopFileSystemPath hadoopFileSystemPath );

  void chmod( HadoopFileSystemPath hadoopFileSystemPath, int permissions ) throws IOException;

  boolean exists( HadoopFileSystemPath path ) throws IOException;

  public HadoopFileSystemPath resolvePath( HadoopFileSystemPath path ) throws IOException;

  String getFsDefaultName();
}
