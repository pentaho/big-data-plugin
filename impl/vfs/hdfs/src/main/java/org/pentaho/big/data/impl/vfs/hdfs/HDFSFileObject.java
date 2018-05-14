/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.impl.vfs.hdfs;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileObject;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileStatus;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystem;


import java.io.InputStream;
import java.io.OutputStream;

public class HDFSFileObject extends AbstractFileObject  {

  private HadoopFileSystem hdfs;

  public HDFSFileObject( final AbstractFileName name, final HDFSFileSystem fileSystem ) throws FileSystemException {
    super( name, fileSystem );
    hdfs = fileSystem.getHDFSFileSystem();
  }

  @Override
  protected long doGetContentSize() throws Exception {
    return hdfs.getFileStatus( hdfs.getPath( getName().getPath() ) ).getLen();
  }

  @Override
  protected OutputStream doGetOutputStream( boolean append ) throws Exception {
    OutputStream out;
    if ( append ) {
      out = hdfs.append( hdfs.getPath( getName().getPath() ) );
    } else {
      out = hdfs.create( hdfs.getPath( getName().getPath() ) );
    }
    return out;
  }

  @Override
  protected InputStream doGetInputStream() throws Exception {
    return hdfs.open( hdfs.getPath( getName().getPath() ) );
  }

  @Override
  protected FileType doGetType() throws Exception {
    HadoopFileStatus status = null;
    try {
      status = hdfs.getFileStatus( hdfs.getPath( getName().getPath() ) );
    } catch ( Exception ex ) {
      // Ignore
    }

    if ( status == null ) {
      return FileType.IMAGINARY;
    } else if ( status.isDir() ) {
      return FileType.FOLDER;
    } else {
      return FileType.FILE;
    }
  }

  @Override
  public void doCreateFolder() throws Exception {
    hdfs.mkdirs( hdfs.getPath( getName().getPath() ) );
  }

  @Override
  public void doDelete() throws Exception {
    hdfs.delete( hdfs.getPath( getName().getPath() ), true );
  }

  @Override
  protected void doRename( FileObject newfile ) throws Exception {
    hdfs.rename( hdfs.getPath( getName().getPath() ), hdfs.getPath( newfile.getName().getPath() ) );
  }

  @Override
  protected long doGetLastModifiedTime() throws Exception {
    return hdfs.getFileStatus( hdfs.getPath( getName().getPath() ) ).getModificationTime();
  }

  @Override
  protected boolean doSetLastModifiedTime( long modtime ) throws Exception {
    hdfs.setTimes( hdfs.getPath( getName().getPath() ), modtime, System.currentTimeMillis() );
    return true;
  }

  @Override
  protected String[] doListChildren() throws Exception {
    HadoopFileStatus[] statusList = hdfs.listStatus( hdfs.getPath( getName().getPath() ) );
    String[] children = new String[ statusList.length ];
    for ( int i = 0; i < statusList.length; i++ ) {
      children[ i ] = statusList[ i ].getPath().getName();
    }
    return children;
  }

}
