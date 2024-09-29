/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
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
  protected InputStream doGetInputStream( final int bufferSize ) throws Exception {
    return this.doGetInputStream();
  }

  @Override
  protected FileType doGetType() throws Exception {
    HadoopFileStatus status = null;
    if ( null == hdfs ) {
      throw new IllegalStateException( "No HDFS file system present" );
    }
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
