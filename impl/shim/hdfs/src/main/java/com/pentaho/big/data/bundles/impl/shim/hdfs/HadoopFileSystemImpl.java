package com.pentaho.big.data.bundles.impl.shim.hdfs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.pentaho.bigdata.api.hdfs.HadoopFileStatus;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystem;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemPath;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by bryan on 5/28/15.
 */
public class HadoopFileSystemImpl implements HadoopFileSystem {
  private final FileSystem fileSystem;

  public HadoopFileSystemImpl( FileSystem fileSystem ) {
    this.fileSystem = fileSystem;
  }

  @Override
  public OutputStream append( HadoopFileSystemPath path ) throws IOException {
    return fileSystem.append( new Path( path.getPath() ) );
  }

  @Override
  public OutputStream create( HadoopFileSystemPath path ) throws IOException {
    return fileSystem.create( new Path( path.getPath() ) );
  }

  @Override
  public boolean delete( HadoopFileSystemPath path, boolean arg1 ) throws IOException {
    return fileSystem.delete( new Path( path.getPath() ), arg1 );
  }

  @Override
  public HadoopFileStatus getFileStatus( HadoopFileSystemPath path ) throws IOException {
    return new HadoopFileStatusImpl( fileSystem.getFileStatus( new Path( path.getPath() ) ) );
  }

  @Override
  public boolean mkdirs( HadoopFileSystemPath path ) throws IOException {
    return fileSystem.mkdirs( new Path( path.getPath() ) );
  }

  @Override
  public InputStream open( HadoopFileSystemPath path ) throws IOException {
    return fileSystem.open( new Path( path.getPath() ) );
  }

  @Override
  public boolean rename( HadoopFileSystemPath path, HadoopFileSystemPath path2 ) throws IOException {
    return fileSystem.rename( new Path( path.getPath() ), new Path( path2.getPath() ) );
  }

  @Override
  public void setTimes( HadoopFileSystemPath path, long mtime, long atime ) throws IOException {
    fileSystem.setTimes( new Path( path.getPath() ), mtime, atime );
  }

  @Override
  public HadoopFileStatus[] listStatus( HadoopFileSystemPath path ) throws IOException {
    FileStatus[] fileStatuses = fileSystem.listStatus( new Path( path.getPath() ) );
    if ( fileStatuses == null ) {
      return null;
    }
    HadoopFileStatus[] result = new HadoopFileStatus[ fileStatuses.length ];
    for ( int i = 0; i < fileStatuses.length; i++ ) {
      result[ i ] = new HadoopFileStatusImpl( fileStatuses[ i ] );
    }
    return result;
  }

  @Override
  public HadoopFileSystemPath getPath( String path ) {
    return new HadoopFileSystemPathImpl( new Path( path ) );
  }

  @Override public HadoopFileSystemPath makeQualified( HadoopFileSystemPath hadoopFileSystemPath ) {
    return new HadoopFileSystemPathImpl( fileSystem
      .makeQualified( HadoopFileSystemPathImpl.toHadoopFileSystemPathImpl( hadoopFileSystemPath ).getRawPath() ) );
  }

  @Override public void chmod( HadoopFileSystemPath hadoopFileSystemPath, int permissions ) throws IOException {
    int owner = permissions / 100;
    if ( owner < 0 || owner > 7 ) {
      throw new IllegalArgumentException( "Expected owner permissions between 0 and 7" );
    }
    int group = ( permissions - ( owner * 100 ) ) / 10;
    if ( group < 0 || group > 7 ) {
      throw new IllegalArgumentException( "Expected group permissions between 0 and 7" );
    }
    int other = permissions - ( owner * 100 ) - ( group * 10 );
    if ( other < 0 || other > 7 ) {
      throw new IllegalArgumentException( "Expected other permissions between 0 and 7" );
    }
    fileSystem.setPermission( HadoopFileSystemPathImpl.toHadoopFileSystemPathImpl( hadoopFileSystemPath ).getRawPath(),
      new FsPermission( FsAction.values()[ owner ], FsAction.values()[ group ], FsAction.values()[ other ] ) );
  }

  @Override public boolean exists( HadoopFileSystemPath path ) throws IOException {
    return fileSystem.exists( HadoopFileSystemPathImpl.toHadoopFileSystemPathImpl( path ).getRawPath() );
  }

  @Override public HadoopFileSystemPath resolvePath( HadoopFileSystemPath path ) throws IOException {
    return new HadoopFileSystemPathImpl(
      fileSystem.getFileStatus( HadoopFileSystemPathImpl.toHadoopFileSystemPathImpl( path ).getRawPath() ).getPath() );
  }

  @Override public String getFsDefaultName() {
    return fileSystem.getConf().get( "fs.defaultFS", fileSystem.getConf().get( "fs.default.name" ) );
  }
}
