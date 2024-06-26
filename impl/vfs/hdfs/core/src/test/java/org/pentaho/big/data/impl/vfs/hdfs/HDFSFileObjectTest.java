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

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileStatus;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystem;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystemPath;


import java.io.InputStream;
import java.io.OutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 8/7/15.
 */
public class HDFSFileObjectTest {
  private AbstractFileName abstractFileName;
  private HDFSFileSystem hdfsFileSystem;
  private HadoopFileSystem hadoopFileSystem;
  private HDFSFileObject hdfsFileObject;
  private HadoopFileSystemPath hadoopFileSystemPath;

  @Before
  public void setup() throws FileSystemException {
    abstractFileName = mock( AbstractFileName.class );
    hadoopFileSystem = mock( HadoopFileSystem.class );
    hdfsFileSystem =
      new HDFSFileSystem( mock( AbstractFileName.class ), null, hadoopFileSystem );
    hdfsFileObject = new HDFSFileObject( abstractFileName, hdfsFileSystem );
    String path = "fake-path";
    hadoopFileSystemPath = mock( HadoopFileSystemPath.class );
    when( abstractFileName.getPath() ).thenReturn( path );
    when( hadoopFileSystem.getPath( path ) ).thenReturn( hadoopFileSystemPath );
  }

  @Test
  public void testGetContentSize() throws Exception {
    long len = 321L;
    HadoopFileStatus hadoopFileStatus = mock( HadoopFileStatus.class );
    when( hadoopFileSystem.getFileStatus( hadoopFileSystemPath ) ).thenReturn( hadoopFileStatus );
    when( hadoopFileStatus.getLen() ).thenReturn( len );
    assertEquals( len, hdfsFileObject.doGetContentSize() );
  }

  @Test
  public void testDoGetOutputStreamAppend() throws Exception {
    OutputStream outputStream = mock( OutputStream.class );
    when( hadoopFileSystem.append( hadoopFileSystemPath ) ).thenReturn( outputStream );
    assertEquals( outputStream, hdfsFileObject.doGetOutputStream( true ) );
  }

  @Test
  public void testDoGetOutputStreamCreate() throws Exception {
    OutputStream outputStream = mock( OutputStream.class );
    when( hadoopFileSystem.create( hadoopFileSystemPath ) ).thenReturn( outputStream );
    assertEquals( outputStream, hdfsFileObject.doGetOutputStream( false ) );
  }

  @Test
  public void testDoGetInputStream() throws Exception {
    InputStream inputStream = mock( InputStream.class );
    when( hadoopFileSystem.open( hadoopFileSystemPath ) ).thenReturn( inputStream );
    assertEquals( inputStream, hdfsFileObject.doGetInputStream() );
  }

  @Test
  public void testDoGetTypeFile() throws Exception {
    HadoopFileStatus hadoopFileStatus = mock( HadoopFileStatus.class );
    when( hadoopFileSystem.getFileStatus( hadoopFileSystemPath ) ).thenReturn( hadoopFileStatus );
    when( hadoopFileStatus.isDir() ).thenReturn( false );
    assertEquals( FileType.FILE, hdfsFileObject.doGetType() );
  }

  @Test
  public void testDoGetTypeFolder() throws Exception {
    HadoopFileStatus hadoopFileStatus = mock( HadoopFileStatus.class );
    when( hadoopFileSystem.getFileStatus( hadoopFileSystemPath ) ).thenReturn( hadoopFileStatus );
    when( hadoopFileStatus.isDir() ).thenReturn( true );
    assertEquals( FileType.FOLDER, hdfsFileObject.doGetType() );
  }

  @Test
  public void testDoGetTypeImaginary() throws Exception {
    assertEquals( FileType.IMAGINARY, hdfsFileObject.doGetType() );
  }

  @Test
  public void testDoCreateFolder() throws Exception {
    hdfsFileObject.doCreateFolder();
    verify( hadoopFileSystem ).mkdirs( hadoopFileSystemPath );
  }

  @Test
  public void testDoRename() throws Exception {
    FileObject fileObject = mock( FileObject.class );
    FileName fileName = mock( FileName.class );
    when( fileObject.getName() ).thenReturn( fileName );
    String path2 = "fake-path-2";
    when( fileName.getPath() ).thenReturn( path2 );
    HadoopFileSystemPath newPath = mock( HadoopFileSystemPath.class );
    when( hadoopFileSystem.getPath( path2 ) ).thenReturn( newPath );
    hdfsFileObject.doRename( fileObject );
    verify( hadoopFileSystem ).rename( hadoopFileSystemPath, newPath );
  }

  @Test
  public void testDoGetLastModifiedTime() throws Exception {
    long modificationTime = 8988L;
    HadoopFileStatus hadoopFileStatus = mock( HadoopFileStatus.class );
    when( hadoopFileSystem.getFileStatus( hadoopFileSystemPath ) ).thenReturn( hadoopFileStatus );
    when( hadoopFileStatus.getModificationTime() ).thenReturn( modificationTime );
    assertEquals( modificationTime, hdfsFileObject.doGetLastModifiedTime() );
  }

  @Test
  public void testDoSetLastModifiedTime() throws Exception {
    long modtime = 48933L;
    long start = System.currentTimeMillis();
    assertTrue( hdfsFileObject.doSetLastModifiedTime( modtime ) );
    ArgumentCaptor<Long> longArgumentCaptor = ArgumentCaptor.forClass( Long.class );
    verify( hadoopFileSystem ).setTimes( eq( hadoopFileSystemPath ), eq( modtime ), longArgumentCaptor.capture() );
    Long accessTime = longArgumentCaptor.getValue();
    assertTrue( start <= accessTime );
    assertTrue( accessTime <= System.currentTimeMillis() );
  }

  @Test
  public void testDoListChildren() throws Exception {
    String childPathName = "fake-path-child";
    testDoListChildrenInternal( childPathName );
  }

  @Test
  public void testDoListChildrenWithSpaces() throws Exception {
    String childPathName = "fake path child with spaces";
    testDoListChildrenInternal( childPathName );
  }

  private void testDoListChildrenInternal( String childPathName ) throws Exception {
    HadoopFileStatus hadoopFileStatus = mock( HadoopFileStatus.class );
    HadoopFileStatus[] hadoopFileStatuses = {
      hadoopFileStatus
    };
    HadoopFileSystemPath childPath = mock( HadoopFileSystemPath.class );
    when( hadoopFileStatus.getPath() ).thenReturn( childPath );
    when( childPath.getName() ).thenReturn( childPathName );
    when( hadoopFileSystem.listStatus( hadoopFileSystemPath ) ).thenReturn( hadoopFileStatuses );
    String[] children = hdfsFileObject.doListChildren();
    assertEquals( 1, children.length );
    assertEquals( childPathName, children[ 0 ] );
  }
}
