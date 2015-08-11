/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package com.pentaho.big.data.bundles.impl.shim.hdfs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
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
  public OutputStream append( final HadoopFileSystemPath path ) throws IOException {
    return callAndWrapExceptions( new IOExceptionCallable<OutputStream>() {
      @Override public OutputStream call() throws IOException {
        return fileSystem.append( new Path( path.getPath() ) );
      }
    } );
  }

  @Override
  public OutputStream create( final HadoopFileSystemPath path ) throws IOException {
    return callAndWrapExceptions( new IOExceptionCallable<OutputStream>() {
      @Override public OutputStream call() throws IOException {
        return fileSystem.create( new Path( path.getPath() ) );
      }
    } );
  }

  @Override
  public boolean delete( final HadoopFileSystemPath path, final boolean arg1 ) throws IOException {
    return callAndWrapExceptions( new IOExceptionCallable<Boolean>() {
      @Override public Boolean call() throws IOException {
        return fileSystem.delete( new Path( path.getPath() ), arg1 );
      }
    } );
  }

  @Override
  public HadoopFileStatus getFileStatus( final HadoopFileSystemPath path ) throws IOException {
    return callAndWrapExceptions( new IOExceptionCallable<HadoopFileStatus>() {
      @Override public HadoopFileStatus call() throws IOException {
        return new HadoopFileStatusImpl( fileSystem.getFileStatus( new Path( path.getPath() ) ) );
      }
    } );
  }

  @Override
  public boolean mkdirs( final HadoopFileSystemPath path ) throws IOException {
    return callAndWrapExceptions( new IOExceptionCallable<Boolean>() {
      @Override public Boolean call() throws IOException {
        return fileSystem.mkdirs( new Path( path.getPath() ) );
      }
    } );
  }

  @Override
  public InputStream open( final HadoopFileSystemPath path ) throws IOException {
    return callAndWrapExceptions( new IOExceptionCallable<InputStream>() {
      @Override public InputStream call() throws IOException {
        return fileSystem.open( new Path( path.getPath() ) );
      }
    } );
  }

  @Override
  public boolean rename( final HadoopFileSystemPath path, final HadoopFileSystemPath path2 ) throws IOException {
    return callAndWrapExceptions( new IOExceptionCallable<Boolean>() {
      @Override public Boolean call() throws IOException {
        return fileSystem.rename( new Path( path.getPath() ), new Path( path2.getPath() ) );
      }
    } );
  }

  @Override
  public void setTimes( final HadoopFileSystemPath path, final long mtime, final long atime ) throws IOException {
    callAndWrapExceptions( new IOExceptionCallable<Void>() {
      @Override public Void call() throws IOException {
        fileSystem.setTimes( new Path( path.getPath() ), mtime, atime );
        return null;
      }
    } );
  }

  @Override
  public HadoopFileStatus[] listStatus( final HadoopFileSystemPath path ) throws IOException {
    FileStatus[] fileStatuses = callAndWrapExceptions( new IOExceptionCallable<FileStatus[]>() {
      @Override public FileStatus[] call() throws IOException {
        return fileSystem.listStatus( new Path( path.getPath() ) );
      }
    } );
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

  @Override public HadoopFileSystemPath getHomeDirectory() {
    return new HadoopFileSystemPathImpl( fileSystem.getHomeDirectory() );
  }

  @Override public HadoopFileSystemPath makeQualified( HadoopFileSystemPath hadoopFileSystemPath ) {
    return new HadoopFileSystemPathImpl( fileSystem
      .makeQualified( HadoopFileSystemPathImpl.toHadoopFileSystemPathImpl( hadoopFileSystemPath ).getRawPath() ) );
  }

  @Override public void chmod( final HadoopFileSystemPath hadoopFileSystemPath, int permissions ) throws IOException {
    final int owner = permissions / 100;
    if ( owner < 0 || owner > 7 ) {
      throw new IllegalArgumentException( "Expected owner permissions between 0 and 7" );
    }
    final int group = ( permissions - ( owner * 100 ) ) / 10;
    if ( group < 0 || group > 7 ) {
      throw new IllegalArgumentException( "Expected group permissions between 0 and 7" );
    }
    final int other = permissions - ( owner * 100 ) - ( group * 10 );
    if ( other < 0 || other > 7 ) {
      throw new IllegalArgumentException( "Expected other permissions between 0 and 7" );
    }
    callAndWrapExceptions( new IOExceptionCallable<Void>() {
      @Override public Void call() throws IOException {
        fileSystem.setPermission(
          HadoopFileSystemPathImpl.toHadoopFileSystemPathImpl( hadoopFileSystemPath ).getRawPath(),
          new FsPermission( FsAction.values()[ owner ], FsAction.values()[ group ], FsAction.values()[ other ] ) );
        return null;
      }
    } );
  }

  @Override public boolean exists( final HadoopFileSystemPath path ) throws IOException {
    return callAndWrapExceptions( new IOExceptionCallable<Boolean>() {
      @Override public Boolean call() throws IOException {
        return fileSystem.exists( HadoopFileSystemPathImpl.toHadoopFileSystemPathImpl( path ).getRawPath() );
      }
    } );
  }

  @Override public HadoopFileSystemPath resolvePath( final HadoopFileSystemPath path ) throws IOException {
    return callAndWrapExceptions( new IOExceptionCallable<HadoopFileSystemPath>() {
      @Override public HadoopFileSystemPath call() throws IOException {
        return new HadoopFileSystemPathImpl(
          fileSystem.getFileStatus( HadoopFileSystemPathImpl.toHadoopFileSystemPathImpl( path ).getRawPath() )
            .getPath() );
      }
    } );
  }

  @Override public String getFsDefaultName() {
    return fileSystem.getConf().get( "fs.defaultFS", fileSystem.getConf().get( "fs.default.name" ) );
  }

  private <T> T callAndWrapExceptions( IOExceptionCallable<T> ioExceptionCallable ) throws IOException {
    try {
      return ioExceptionCallable.call();
    } catch ( AccessControlException e ) {
      throw new org.pentaho.bigdata.api.hdfs.exceptions.AccessControlException( e.getMessage(), e );
    }
  }

  private interface IOExceptionCallable<T> {
    T call() throws IOException;
  }
}
