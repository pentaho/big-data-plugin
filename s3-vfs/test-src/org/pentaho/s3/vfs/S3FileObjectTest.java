/*!
* Copyright 2010 - 2017 Pentaho Corporation.  All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
*/
package org.pentaho.s3.vfs;

import org.apache.commons.vfs2.CacheStrategy;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.FilesCache;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.provider.VfsComponentContext;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.OutputStream;
import java.util.Calendar;

import static org.junit.Assert.*;

import static org.mockito.Mockito.*;

/**
 * created by: dzmitry_bahdanovich date: 10/18/13
 */

public class S3FileObjectTest {

  private static final String S3OBJECT_NAME = "TEST_NAME";
  public static final String awsAccessKey = "ABC123456DEF7890"; // fake out a key
  public static final String awsSecretKey = "A+123456BCD99/99999999ZZZ+B"; // fake out a secret key
  public static final String HOST = "S3";
  public static final String SCHEME = "s3";
  public static final int PORT = 843;

  public static final String BUCKET_NAME = "bucket";
  public static final String OBJECT_NAME = "obj";

  private S3FileName filename;
  private S3FileSystem fileSystemSpy;
  private S3FileObject s3FileObjectSpy;
  private S3Service s3ServiceMock;
  private static final String S3VFS_USE_TEMPORARY_FILE_ON_UPLOAD_DATA = "s3.vfs.useTempFileOnUploadData";
  private S3DataContent s3DataContent;

  @Before
  public void setUp() throws Exception {

    s3ServiceMock = mock( S3Service.class );
    S3Bucket testBucket = new S3Bucket( BUCKET_NAME );
    S3Object s3Object = new S3Object( OBJECT_NAME );

    filename = new S3FileName( SCHEME, HOST, PORT, PORT, awsAccessKey, awsSecretKey, "/" + BUCKET_NAME + "/" + OBJECT_NAME, FileType.FILE, null );
    S3FileName rootFileName = new S3FileName( SCHEME, HOST, PORT, PORT, awsAccessKey, awsSecretKey, "/" + BUCKET_NAME, FileType.FILE, null );
    S3FileSystem fileSystem = new S3FileSystem( rootFileName, new FileSystemOptions() );
    fileSystemSpy = spy( fileSystem );
    VfsComponentContext context = mock( VfsComponentContext.class );
    final DefaultFileSystemManager fsm = new DefaultFileSystemManager();
    FilesCache cache = mock( FilesCache.class );
    fsm.setFilesCache( cache );
    fsm.setCacheStrategy( CacheStrategy.ON_RESOLVE );
    when( context.getFileSystemManager() ).thenReturn( fsm );
    fileSystemSpy.setContext( context );

    S3FileObject s3FileObject = new S3FileObject( filename, fileSystemSpy );
    s3FileObjectSpy = spy( s3FileObject );

    // specify the behaviour of S3 Service
    when( s3ServiceMock.getBucket( BUCKET_NAME ) ).thenReturn( testBucket );
    when( s3ServiceMock.getObject( testBucket, OBJECT_NAME ) ).thenReturn( s3Object );
    when( s3ServiceMock.getObject( BUCKET_NAME, OBJECT_NAME ) ).thenReturn( s3Object );
    when( s3ServiceMock.createBucket( BUCKET_NAME ) ).thenThrow( new S3ServiceException() ); // throw exception if
                                                                                             // bucket exists
    when( fileSystemSpy.getS3Service() ).thenReturn( s3ServiceMock );
    when( s3FileObjectSpy.getS3Bucket() ).thenReturn( testBucket );

  }

  @After
  public void tearDown() throws Exception {
    s3DataContent = null;
  }

  @Test
  public void testGetS3ObjectNotDeleteIfExists() throws Exception {
    testGetS3ObjectWithFlag( false );
  }

  @Test
  public void testGetS3ObjectDeleteIfExists() throws Exception {
    testGetS3ObjectWithFlag( true );
  }

  @Test
  public void testGetS3BucketName() {
    filename = new S3FileName( SCHEME, HOST, PORT, PORT, awsAccessKey, awsSecretKey, "/" + BUCKET_NAME, FileType.FOLDER, null );
    when( s3FileObjectSpy.getName() ).thenReturn( filename );
    s3FileObjectSpy.getS3BucketName();
  }

  @Test
  public void testGetS3BucketNoService() throws Exception {

    when( fileSystemSpy.getS3Service() ).thenReturn( null );
    s3FileObjectSpy = spy( new S3FileObject( filename, fileSystemSpy ) );
    s3FileObjectSpy.getS3Bucket();
  }

  @Test
  public void testGetS3ObjectNoPath() throws Exception {
    FileName mockFile = mock( FileName.class );
    when( s3FileObjectSpy.getName() ).thenReturn( mockFile );
    when( mockFile.getPath() ).thenReturn( "" );
    assertNull( s3FileObjectSpy.getS3Object( false, false ) );
  }

  @Test
  public void testGetS3ObjectWithException() throws Exception {

    when( s3FileObjectSpy.getS3BucketName() ).thenThrow( new NullPointerException() );
    assertNotNull( s3FileObjectSpy.getS3Object( true, false ) );
    when( s3FileObjectSpy.getName() ).thenThrow( new NullPointerException() );
    assertNull( s3FileObjectSpy.getS3Object( true, false ) );
  }

  @Test
  public void testDoGetContentSize() throws Exception {
    assertEquals( 0, s3FileObjectSpy.doGetContentSize() );
  }

  @Test
  public void testDoGetOutputStream() throws Exception {
    assertNotNull( s3FileObjectSpy.doGetOutputStream( false ) );
    OutputStream out = s3FileObjectSpy.doGetOutputStream( true );
    assertNotNull( out );
    out.close();
  }

  @Test
  public void testDoGetInputStream() throws Exception {
    assertNull( s3FileObjectSpy.doGetInputStream() );
  }

  @Test
  public void testDoGetTypeImaginary() throws Exception {
    assertEquals( FileType.IMAGINARY, s3FileObjectSpy.doGetType() );
    when( s3FileObjectSpy.getS3Bucket() ).thenThrow( new Exception() );
    assertEquals( FileType.IMAGINARY, s3FileObjectSpy.doGetType() );
  }

  @Test
  public void testDoGetTypeFolder() throws Exception {
    FileName mockFile = mock( FileName.class );
    when( s3FileObjectSpy.getName() ).thenReturn( mockFile );
    when( mockFile.getPath() ).thenReturn( S3FileObject.DELIMITER );
    assertEquals( FileType.FOLDER, s3FileObjectSpy.doGetType() );
  }

  @Test
  public void testDoCreateFolder() throws Exception {
    doReturn( s3FileObjectSpy ).when( fileSystemSpy ).resolveFile( filename );
    s3FileObjectSpy.doCreateFolder();
    when( s3FileObjectSpy.getS3Object( false, false ) ).thenReturn( null );
    s3FileObjectSpy.doCreateFolder();
  }

  @Test
  public void testCanRenameTo() throws Exception {
    FileObject newFile = mock( FileObject.class );
    assertFalse( s3FileObjectSpy.canRenameTo( newFile ) );
    when( s3FileObjectSpy.getType() ).thenReturn( FileType.FOLDER );
    assertFalse( s3FileObjectSpy.canRenameTo( newFile ) );
  }

  @Test( expected = NullPointerException.class )
  public void testCanRenameToNullFile() throws Exception {
    // This is a bug / weakness in VFS itself
    s3FileObjectSpy.canRenameTo( null );
  }

  @Test
  public void testDoDelete() throws Exception {
    s3FileObjectSpy.doDelete();
    when( s3FileObjectSpy.getS3Object( false, false ) ).thenReturn( null );
    s3FileObjectSpy.doDelete();
    when( s3FileObjectSpy.getS3Bucket() ).thenReturn( null );
    s3FileObjectSpy.doDelete();
  }

  @Test
  public void testDoRename() throws Exception {
    FileObject newFile = mock( FileObject.class );
    when( newFile.getName() ).thenReturn( filename );
    s3FileObjectSpy.doRename( newFile );
  }

  @Test
  public void testDoGetLastModifiedTime() throws Exception {
    S3Object object = mock( S3Object.class );
    when( s3FileObjectSpy.getS3Object( false, false ) ).thenReturn( object );
    when( object.getLastModifiedDate() ).thenReturn( Calendar.getInstance().getTime() );
    assertTrue( s3FileObjectSpy.doGetLastModifiedTime() > 0 );
    when( s3FileObjectSpy.getType() ).thenReturn( FileType.FOLDER );
    assertEquals( -1, s3FileObjectSpy.doGetLastModifiedTime() );
    // This is a no-op method that returns true
    assertTrue( s3FileObjectSpy.doSetLastModifiedTime( 1L ) );
  }

  @Test
  public void testDoListChildren() throws Exception {
    assertNull( s3FileObjectSpy.doListChildren() );
    FileName mockFile = mock( FileName.class );
    when( s3FileObjectSpy.getName() ).thenReturn( mockFile );
    when( mockFile.getPath() ).thenReturn( S3FileObject.DELIMITER );
    when( s3FileObjectSpy.getS3Bucket() ).thenReturn( null );
    S3Bucket bucket = mock( S3Bucket.class );
    S3Bucket[] buckets = new S3Bucket[] { bucket };
    when( s3ServiceMock.listAllBuckets() ).thenReturn( buckets );
    String[] children = s3FileObjectSpy.doListChildren();
    assertNotNull( children );

    when( mockFile.getPath() ).thenReturn( "/path/to/myFile.txt" );
    S3Object object = mock( S3Object.class );
    when( object.getKey() ).thenReturn( "/path/to/myFile.txt@S3/" );
    S3Object[] objects = new S3Object[] { object };
    when( s3ServiceMock.listObjects( anyString(), anyString(), anyString() ) ).thenReturn( objects );
    children = s3FileObjectSpy.doListChildren();
    assertNotNull( children );
  }

  @Test
  public void testHandleCreate() throws Exception {
    s3FileObjectSpy.handleCreate( FileType.FILE );
  }

  @Test
  public void testHandleDelete() throws Exception {
    s3FileObjectSpy.handleDelete();
  }

  protected void testGetS3ObjectWithFlag( boolean deleteIfExists ) throws Exception {
    S3FileObject s3FileObject = new S3FileObject( filename, fileSystemSpy );
    S3Object s3Object = s3FileObject.getS3Object( deleteIfExists, false );
    assertNotNull( s3Object );
  }

  @Test
  public void testGetS3ObjectWithS3DataContent_AsFile() throws Exception {
    turnOnUseTemporaryFileOnUploadData();
    s3DataContent = new S3DataContent();
    s3DataContent.load();
    S3Object s3Object = S3FileObject.getS3Object( S3OBJECT_NAME, s3DataContent );
    assertNotNull( s3Object );
    assertEquals( S3OBJECT_NAME, s3Object.getName() );
    assertEquals( s3DataContent.asFile().length(), s3Object.getContentLength() );
    assertEquals( s3DataContent.asFile(), s3Object.getDataInputFile() );
  }

  @Test
  public void testGetS3ObjectWithS3DataContent_AsStream() throws Exception {
    String testData = "1, 2, 3";
    turnOffUseTemporaryFileOnUploadData();
    s3DataContent = new S3DataContent();
    s3DataContent.load();
    s3DataContent.asByteArrayStream().write( testData.getBytes() );
    S3Object s3Object = S3FileObject.getS3Object( S3OBJECT_NAME, s3DataContent );
    assertNotNull( s3Object );
    assertEquals( S3OBJECT_NAME, s3Object.getName() );
    assertEquals( s3DataContent.asByteArrayStream().size(), s3Object.getContentLength() );
    assertNull( s3Object.getDataInputFile() );
    assertNotNull( s3Object.getDataInputStream() );
  }

  private static void turnOnUseTemporaryFileOnUploadData() {
    System.getProperties().put( S3VFS_USE_TEMPORARY_FILE_ON_UPLOAD_DATA, "Y" );
  }

  private static void turnOffUseTemporaryFileOnUploadData() {
    System.getProperties().put( S3VFS_USE_TEMPORARY_FILE_ON_UPLOAD_DATA, "N" );
  }

}
