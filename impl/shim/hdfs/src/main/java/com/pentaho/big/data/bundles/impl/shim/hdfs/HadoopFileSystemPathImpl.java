package com.pentaho.big.data.bundles.impl.shim.hdfs;

import org.apache.hadoop.fs.Path;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemPath;

import java.net.URI;

/**
 * Created by bryan on 5/28/15.
 */
public class HadoopFileSystemPathImpl implements HadoopFileSystemPath {
  private final Path path;

  public HadoopFileSystemPathImpl( Path path ) {
    this.path = path;
  }

  public static HadoopFileSystemPathImpl toHadoopFileSystemPathImpl( HadoopFileSystemPath hadoopFileSystemPath ) {
    if ( hadoopFileSystemPath instanceof HadoopFileSystemPathImpl ) {
      return (HadoopFileSystemPathImpl) hadoopFileSystemPath;
    } else if ( hadoopFileSystemPath == null ) {
      return null;
    }
    return new HadoopFileSystemPathImpl( new Path( hadoopFileSystemPath.toString() ) );
  }

  @Override
  public String getPath() {
    return path.toUri().getPath();
  }

  @Override public String getName() {
    return path.getName();
  }

  @Override public String toString() {
    return path.toString();
  }

  @Override public URI toUri() {
    return path.toUri();
  }

  @Override public HadoopFileSystemPath resolve( HadoopFileSystemPath child ) {
    return new HadoopFileSystemPathImpl( new Path( path, toHadoopFileSystemPathImpl( child ).getRawPath() ) );
  }

  @Override public HadoopFileSystemPath resolve( String child ) {
    return new HadoopFileSystemPathImpl( new Path( path, child ) );
  }

  @Override public HadoopFileSystemPath getParent() {
    return new HadoopFileSystemPathImpl( path.getParent() );
  }

  public Path getRawPath() {
    return path;
  }
}
