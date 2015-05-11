/*!
* Copyright 2010 - 2013 Pentaho Corporation.  All rights reserved.
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

import org.apache.commons.vfs.FileSystemOptions;
import org.apache.commons.vfs.FileType;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import static org.mockito.Mockito.*;

/**
 * created by: dzmitry_bahdanovich date:       10/18/13
 */

public class S3FileObjectTest {

  public static final String awsAccessKey = "ABC123456DEF7890";             // fake out a key
  public static final String awsSecretKey = "A+123456BCD99/99999999ZZZ+B";   // fake out a secret key
  public static final String HOST = "S3";
  public static final String SCHEME = "s3";
  public static final int PORT = 843;

  public static final String BUCKET_NAME = "bucket";
  public static final String OBJECT_NAME = "obj";

  private S3FileName filename;
  private S3FileSystem fileSystemSpy;
  private S3FileObject s3FileObjectSpy;

  @Before
  public void setUp() throws Exception {

    S3Service s3ServiceMock = mock( S3Service.class );
    S3Bucket testBucket = new S3Bucket( BUCKET_NAME );
    S3Object s3Object = new S3Object( OBJECT_NAME );
    filename =
      new S3FileName( SCHEME, HOST, PORT, PORT, awsAccessKey, awsSecretKey, "/" + BUCKET_NAME + "/" + OBJECT_NAME,
        FileType.FILE, null );
    S3FileName rootFileName =
      new S3FileName( SCHEME, HOST, PORT, PORT, awsAccessKey, awsSecretKey, "/" + BUCKET_NAME, FileType.FILE, null );
    S3FileSystem fileSystem = new S3FileSystem( rootFileName, new FileSystemOptions() );
    fileSystemSpy = spy( fileSystem );
    S3FileObject s3FileObject = new S3FileObject( filename, fileSystemSpy );
    s3FileObjectSpy = spy( s3FileObject );

    //specify the behaviour of S3 Service
    when( s3ServiceMock.getBucket( BUCKET_NAME ) ).thenReturn( testBucket );
    when( s3ServiceMock.getObject( testBucket, OBJECT_NAME ) ).thenReturn( s3Object );
    when( s3ServiceMock.getObject( BUCKET_NAME, OBJECT_NAME ) ).thenReturn( s3Object );
    when( s3ServiceMock.createBucket( BUCKET_NAME ) )
      .thenThrow( new S3ServiceException() ); // throw exception if bucket exists
    when( fileSystemSpy.getS3Service() ).thenReturn( s3ServiceMock );
    when( s3FileObjectSpy.getS3Bucket() ).thenReturn( testBucket );

  }

  @After
  public void tearDown() throws Exception {

  }

  @Test
  public void testGetS3ObjectNotDeleteIfExists() throws Exception {
    testGetS3ObjectWithFlag( false );
  }

  @Test
  public void testGetS3ObjectDeleteIfExists() throws Exception {
    testGetS3ObjectWithFlag( true );
  }


  private void testGetS3ObjectWithFlag( boolean deleteIfExists ) throws Exception {
    S3FileObject s3FileObject = new S3FileObject( filename, fileSystemSpy );
    S3Object s3Object = s3FileObject.getS3Object( deleteIfExists );
    assertNotNull( s3Object );
  }
}
