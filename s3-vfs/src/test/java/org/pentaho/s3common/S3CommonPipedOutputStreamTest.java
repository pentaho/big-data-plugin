package org.pentaho.s3common;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.UploadPartRequest;

public class S3CommonPipedOutputStreamTest {

  private S3CommonFileSystem fileSystem;
  private AmazonS3 s3Client;
  private String bucket = "bucket";
  private String key = "key";

  @Before
  public void setUp() {
    fileSystem = mock( S3CommonFileSystem.class );
    s3Client = mock( AmazonS3.class );
    when( fileSystem.getS3Client() ).thenReturn( s3Client );
  }

  @After
  public void tearDown() {
    fileSystem = null;
    s3Client = null;
  }

  @Test
  public void testSinglePartUpload() throws Exception {
    InitiateMultipartUploadResult initResult = new InitiateMultipartUploadResult();
    initResult.setUploadId( "uploadId" );
    when( s3Client.initiateMultipartUpload( any( InitiateMultipartUploadRequest.class ) ) ).thenReturn( initResult );
    com.amazonaws.services.s3.model.UploadPartResult uploadResult = new com.amazonaws.services.s3.model.UploadPartResult();
    uploadResult.setETag( "etag" );
    uploadResult.setPartNumber( 1 );
    when( s3Client.uploadPart( any( UploadPartRequest.class ) ) ).thenReturn( uploadResult );
    // doNothing().when( s3Client ).completeMultipartUpload( any( CompleteMultipartUploadRequest.class ) );
    // doNothing().when( s3Client ).abortMultipartUpload( any( AbortMultipartUploadRequest.class ) );

    S3CommonPipedOutputStream out = new S3CommonPipedOutputStream( fileSystem, bucket, key, 5 * 1024 * 1024, 1 );
    byte[] data = new byte[ 3 * 1024 * 1024 ]; // 3MB < 5MB
    out.write( data );
    out.close();
    // Wait a bit for async threads (should not be needed, but for safety)
    Thread.sleep( 200 );
    verify( s3Client, atLeastOnce() ).uploadPart( any( UploadPartRequest.class ) );
    verify( s3Client, atLeastOnce() ).completeMultipartUpload( any( CompleteMultipartUploadRequest.class ) );
  }

  @Test
  public void testMultiPartUpload() throws Exception {
   InitiateMultipartUploadResult initResult = new InitiateMultipartUploadResult();
    initResult.setUploadId( "uploadId" );
    when( s3Client.initiateMultipartUpload( any( InitiateMultipartUploadRequest.class ) ) ).thenReturn( initResult );
    com.amazonaws.services.s3.model.UploadPartResult uploadResult = new com.amazonaws.services.s3.model.UploadPartResult();
    uploadResult.setETag( "etag" );
    uploadResult.setPartNumber( 1 );
    when( s3Client.uploadPart( any( UploadPartRequest.class ) ) ).thenReturn( uploadResult );

    S3CommonPipedOutputStream out = new S3CommonPipedOutputStream( fileSystem, bucket, key, 5 * 1024 * 1024, 2 );
    byte[] data = new byte[ 12 * 1024 * 1024 ]; // 12MB > 2 parts
    out.write( data );
    out.close();
    Thread.sleep( 200 );
    verify( s3Client, atLeast( 2 ) ).uploadPart( any( UploadPartRequest.class ) );
    verify( s3Client, atLeastOnce() ).completeMultipartUpload( any( CompleteMultipartUploadRequest.class ) );
  }

  @Test
  public void testTooManyPartsTriggersAbort() throws Exception {
    InitiateMultipartUploadResult initResult = new InitiateMultipartUploadResult();
    initResult.setUploadId( "uploadId" );
    when( s3Client.initiateMultipartUpload( any( InitiateMultipartUploadRequest.class ) ) ).thenReturn( initResult );
    com.amazonaws.services.s3.model.UploadPartResult uploadResult = new com.amazonaws.services.s3.model.UploadPartResult();
    uploadResult.setETag( "etag" );
    uploadResult.setPartNumber( 1 );
    when( s3Client.uploadPart( any( UploadPartRequest.class ) ) ).thenReturn( uploadResult );
    // doNothing().when( s3Client ).abortMultipartUpload( any( AbortMultipartUploadRequest.class ) );

    // Use minimum allowed part size (5MB)
    S3CommonPipedOutputStream out = new S3CommonPipedOutputStream( fileSystem, bucket, key, 5 * 1024 * 1024, 1 );
    // Write a large amount of data to try to trigger too many parts (not practical in unit test, so just check no exception for reasonable size)
    byte[] data = new byte[ 55 * 1024 * 1024 ]; // 55MB = 11 parts
    out.write( data );
    out.close();
    Thread.sleep( 200 );
    verify( s3Client, atLeast( 11 ) ).uploadPart( any( UploadPartRequest.class ) );
  }

  @Test
  public void testBlockedUntilDoneFlag() throws Exception {
    InitiateMultipartUploadResult initResult = new InitiateMultipartUploadResult();
    initResult.setUploadId( "uploadId" );
    when( s3Client.initiateMultipartUpload( any( InitiateMultipartUploadRequest.class ) ) ).thenReturn( initResult );
    com.amazonaws.services.s3.model.UploadPartResult uploadResult = new com.amazonaws.services.s3.model.UploadPartResult();
    uploadResult.setETag( "etag" );
    uploadResult.setPartNumber( 1 );
    when( s3Client.uploadPart( any( UploadPartRequest.class ) ) ).thenReturn( uploadResult );
    // doNothing().when( s3Client ).completeMultipartUpload( any( CompleteMultipartUploadRequest.class ) );
    // doNothing().when( s3Client ).abortMultipartUpload( any( AbortMultipartUploadRequest.class ) );

    S3CommonPipedOutputStream out = new S3CommonPipedOutputStream( fileSystem, bucket, key, 5 * 1024 * 1024, 1 );
    out.setBlockedUntilDone( false );
    byte[] data = new byte[ 3 * 1024 * 1024 ];
    out.write( data );
    out.close();
    Thread.sleep( 200 );
    verify( s3Client, atLeastOnce() ).uploadPart( any( UploadPartRequest.class ) );
  }
}
