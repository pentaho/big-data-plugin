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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.bigdata.api.hdfs.HadoopFileStatus;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemPath;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 8/3/15.
 */
public class HadoopFileSystemImplTest {
  private FileSystem fileSystem;
  private HadoopFileSystemCallable hadoopFileSystemCallable;
  private HadoopFileSystemImpl hadoopFileSystem;
  private HadoopFileSystemPath hadoopFileSystemPath;
  private FSDataOutputStream outputStream;
  private String pathString;
  private FSDataInputStream inputStream;
  private HadoopFileSystemPath hadoopFileSystemPath2;
  private String pathString2;
  private Configuration configuration;

  @Before
  public void setup() {
    fileSystem = mock( FileSystem.class );
    configuration = mock( Configuration.class );
    when( fileSystem.getConf() ).thenReturn( configuration );
    hadoopFileSystemCallable = mock( HadoopFileSystemCallable.class );
    hadoopFileSystem = new HadoopFileSystemImpl( hadoopFileSystemCallable );
    outputStream = mock( FSDataOutputStream.class );
    inputStream = mock( FSDataInputStream.class );
    hadoopFileSystemPath = mock( HadoopFileSystemPath.class );
    hadoopFileSystemPath2 = mock( HadoopFileSystemPath.class );
    pathString = "test/path";
    pathString2 = "test/path2";
    when( hadoopFileSystemPath.getPath() ).thenReturn( pathString );
    when( hadoopFileSystemPath2.getPath() ).thenReturn( pathString2 );
    when( hadoopFileSystemCallable.getFileSystem() ).thenReturn( fileSystem );
  }

  @Test
  public void testAppend() throws IOException {
    when( fileSystem.append( eq( new Path( pathString ) ) ) ).thenReturn( outputStream );
    assertEquals( outputStream, hadoopFileSystem.append( hadoopFileSystemPath ) );
  }

  @Test
  public void testCreate() throws IOException {
    when( fileSystem.create( eq( new Path( pathString ) ) ) ).thenReturn( outputStream );
    assertEquals( outputStream, hadoopFileSystem.create( hadoopFileSystemPath ) );
  }

  @Test
  public void testDelete() throws IOException {
    when( fileSystem.delete( eq( new Path( pathString ) ), eq( false ) ) ).thenReturn( true ).thenReturn( false );
    assertEquals( true, hadoopFileSystem.delete( hadoopFileSystemPath, false ) );
    assertEquals( false, hadoopFileSystem.delete( hadoopFileSystemPath, false ) );
  }

  @Test
  public void testGetFileStatus() throws IOException {
    FileStatus fileStatus = mock( FileStatus.class );
    long len = 12345L;
    when( fileStatus.getLen() ).thenReturn( len );
    when( fileSystem.getFileStatus( eq( new Path( pathString ) ) ) ).thenReturn( fileStatus );
    assertEquals( len, hadoopFileSystem.getFileStatus( hadoopFileSystemPath ).getLen() );
  }

  @Test
  public void testMkdirs() throws IOException {
    when( fileSystem.mkdirs( eq( new Path( pathString ) ) ) ).thenReturn( true ).thenReturn( false );
    assertEquals( true, hadoopFileSystem.mkdirs( hadoopFileSystemPath ) );
    assertEquals( false, hadoopFileSystem.mkdirs( hadoopFileSystemPath ) );
  }

  @Test
  public void testOpen() throws IOException {
    when( fileSystem.open( eq( new Path( pathString ) ) ) ).thenReturn( inputStream );
    assertEquals( inputStream, hadoopFileSystem.open( hadoopFileSystemPath ) );
  }

  @Test
  public void testRename() throws IOException {
    when( fileSystem.rename( eq( new Path( pathString ) ), eq( new Path( pathString2 ) ) ) ).thenReturn( true )
      .thenReturn( false );
    assertEquals( true, hadoopFileSystem.rename( hadoopFileSystemPath, hadoopFileSystemPath2 ) );
    assertEquals( false, hadoopFileSystem.rename( hadoopFileSystemPath, hadoopFileSystemPath2 ) );
  }

  @Test
  public void testSetTimes() throws IOException {
    long mtime = 1L;
    long atime = 2L;
    hadoopFileSystem.setTimes( hadoopFileSystemPath, mtime, atime );
    verify( fileSystem ).setTimes( eq( new Path( pathString ) ), eq( mtime ), eq( atime ) );
  }

  @Test
  public void testListStatusNullStatuses() throws IOException {
    when( fileSystem.listStatus( eq( new Path( pathString ) ) ) ).thenReturn( null );
    assertNull( hadoopFileSystem.listStatus( hadoopFileSystemPath ) );
  }

  @Test
  public void testListStatus() throws IOException {
    FileStatus fileStatus = mock( FileStatus.class );
    long len = 54321L;
    when( fileStatus.getLen() ).thenReturn( len );
    when( fileSystem.listStatus( eq( new Path( pathString ) ) ) ).thenReturn( new FileStatus[] { fileStatus } );
    HadoopFileStatus[] hadoopFileStatuses = hadoopFileSystem.listStatus( hadoopFileSystemPath );
    assertEquals( 1, hadoopFileStatuses.length );
    assertEquals( len, hadoopFileStatuses[ 0 ].getLen() );
  }

  @Test
  public void testGetPath() {
    assertEquals( pathString, hadoopFileSystem.getPath( pathString ).getPath() );
  }

  @Test
  public void testMakeQualified() {
    when( hadoopFileSystemPath.toString() ).thenReturn( pathString );
    when( fileSystem.makeQualified( eq( new Path( pathString ) ) ) ).thenReturn( new Path(
      pathString2 ) );
    assertEquals( pathString2, hadoopFileSystem.makeQualified( hadoopFileSystemPath ).getPath() );
  }

  @Test( expected = IllegalArgumentException.class )
  public void testChmodIllegalOwnerUp() throws IOException {
    hadoopFileSystem.chmod( hadoopFileSystemPath, 800 );
  }

  @Test( expected = IllegalArgumentException.class )
  public void testChmodIllegalOwnerDown() throws IOException {
    hadoopFileSystem.chmod( hadoopFileSystemPath, -100 );
  }

  @Test( expected = IllegalArgumentException.class )
  public void testChmodIllegalGroupUp() throws IOException {
    hadoopFileSystem.chmod( hadoopFileSystemPath, 80 );
  }

  @Test( expected = IllegalArgumentException.class )
  public void testChmodIllegalGroupDown() throws IOException {
    hadoopFileSystem.chmod( hadoopFileSystemPath, -10 );
  }

  @Test( expected = IllegalArgumentException.class )
  public void testChmodIllegalOtherUp() throws IOException {
    hadoopFileSystem.chmod( hadoopFileSystemPath, 8 );
  }

  @Test( expected = IllegalArgumentException.class )
  public void testChmodIllegalOtherDown() throws IOException {
    hadoopFileSystem.chmod( hadoopFileSystemPath, -1 );
  }

  @Test
  public void testChmod() throws IOException {
    when( hadoopFileSystemPath.toString() ).thenReturn( pathString );
    hadoopFileSystem.chmod( hadoopFileSystemPath, 753 );
    verify( fileSystem ).setPermission( eq( new Path( pathString ) ),
      eq( new FsPermission( FsAction.ALL, FsAction.READ_EXECUTE, FsAction.WRITE_EXECUTE ) ) );
  }

  @Test
  public void testExists() throws IOException {
    when( hadoopFileSystemPath.toString() ).thenReturn( pathString );
    when( fileSystem.exists( eq( new Path( pathString ) ) ) ).thenReturn( true ).thenReturn( false );
    assertTrue( hadoopFileSystem.exists( hadoopFileSystemPath ) );
    assertFalse( hadoopFileSystem.exists( hadoopFileSystemPath ) );
  }

  @Test
  public void testResolvePath() throws IOException {
    when( hadoopFileSystemPath.toString() ).thenReturn( pathString );
    FileStatus fileStatus = mock( FileStatus.class );
    when( fileStatus.getPath() ).thenReturn( new Path( pathString2 ) );
    when( fileSystem.getFileStatus( eq( new Path( pathString ) ) ) ).thenReturn( fileStatus );
    assertEquals( pathString2, hadoopFileSystem.resolvePath( hadoopFileSystemPath ).getPath() );
  }

  @Test
  public void testGetFsDefaultName() {
    String value = "result";
    String fake = "fake";
    when( configuration.get( "fs.default.name" ) ).thenReturn( fake );
    when( configuration.get( "fs.defaultFS", fake ) ).thenReturn( value );
    assertEquals( value, hadoopFileSystem.getFsDefaultName() );
  }

  @Test
  public void testGetProperty() throws Exception {
    String value = "value";
    String defValue = "defValue";
    String name = "name";
    when( configuration.get( eq( name ) ) ).thenReturn( value );
    when( configuration.get( eq( name ), eq( (String) null ) ) ).thenReturn( value );
    when( configuration.get( eq( name ), eq( defValue ) ) ).thenReturn( defValue );
    assertEquals( value, hadoopFileSystem.getProperty( name, null ) );
    assertEquals( defValue, hadoopFileSystem.getProperty( name, defValue ) );
  }

  @Test
  public void testSetProperty() throws Exception {
    String value = "value";
    String name = "name";
    hadoopFileSystem.setProperty( name, value );
    verify( configuration ).set( name, value );
  }
}
