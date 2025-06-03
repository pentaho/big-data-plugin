package org.pentaho.s3common;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.vfs2.FileSystemException;
import org.pentaho.s3.vfs.S3FileObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyPartRequest;
import com.amazonaws.services.s3.model.CopyPartResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;

/**
 * Utility for performing S3→S3 multipart copy operations, with support for multithreading.
 * Refactored for readability and testability.
 */
public class S3CommonMultipartCopier {

  private static final Class<?> PKG = S3CommonMultipartCopier.class;
  private static final Logger logger = LoggerFactory.getLogger( PKG );

  static final int DEFAULT_PART_SIZE = 100 * 1024 * 1024; // 100MB, as recomended by AWS
  static final int DEFAULT_THREAD_POOL_SIZE = 4;

  private final int partSize;
  private final int threadPoolSize;

  public S3CommonMultipartCopier() {
    this( DEFAULT_PART_SIZE, DEFAULT_THREAD_POOL_SIZE );
  }

  public S3CommonMultipartCopier( int partSize, int threadPoolSize ) {
    if ( partSize < 5 * 1024 * 1024 ) { // S3 minimum part size is 5MB
      throw new IllegalArgumentException( "partSize must be at least 5MB" );
    }
    if ( threadPoolSize < 1 ) {
      throw new IllegalArgumentException( "threadPoolSize must be at least 1" );
    }
    this.partSize = partSize;
    this.threadPoolSize = threadPoolSize;
  }

  /**
   * Perform S3→S3 multipart copy with a custom thread pool size.
   *
   * @param src           Source S3FileObject
   * @param dst           Destination S3FileObject
   * @param logger        Logger for logging
   * @param partSize      Size of each part in bytes (default is 100MB)
   * @param threadPoolSize Number of threads for multipart copy
   * @throws FileSystemException if copy fails
   */
  public void multipartCopy( S3FileObject src, S3FileObject dst ) throws FileSystemException {
    String srcBucket = src.bucketName;
    String srcKey = src.key;
    String dstBucket = dst.bucketName;
    String dstKey = dst.key;

    try {
      ObjectMetadata srcMeta = getObjectMetadata( src, srcBucket, srcKey );

      long contentLength = srcMeta.getContentLength();
      if ( contentLength < partSize ) {
        performSimpleCopy( src, dst, srcBucket, srcKey, dstBucket, dstKey );
      } else {
        performMultipartCopy( src, dst, srcBucket, srcKey, dstBucket, dstKey, contentLength );
      }
    } catch ( AmazonS3Exception e ) {
      logger.error( "S3→S3 server-side copy failed: {} → {}: {}", src.getQualifiedName(), dst.getQualifiedName(), e.getMessage(), e );
      throw new FileSystemException( "vfs.provider.s3/copy-server-side.error", src.getQualifiedName(), dst.getQualifiedName(), e );
    } catch ( Exception e ) {
      logger.error( "Unexpected error during S3→S3 server-side copy: {} → {}: {}", src.getQualifiedName(), dst.getQualifiedName(), e.getMessage(), e );
      throw new FileSystemException( "vfs.provider.s3/copy-server-side.error", src.getQualifiedName(), dst.getQualifiedName(), e );
    }
  }

  private static ObjectMetadata getObjectMetadata( S3FileObject src, String bucket, String key ) {
    return src.fileSystem.getS3Client().getObjectMetadata( bucket, key );
  }

  private static void performSimpleCopy( S3FileObject src, S3FileObject dst, String srcBucket, String srcKey, String dstBucket, String dstKey ) {
    CopyObjectRequest copyRequest = new CopyObjectRequest( srcBucket, srcKey, dstBucket, dstKey );
    dst.fileSystem.getS3Client().copyObject( copyRequest );
    logger.info( "S3→S3 server-side copy succeeded: {} → {}", src.getQualifiedName(), dst.getQualifiedName() );
  }

  private void performMultipartCopy( S3FileObject src, S3FileObject dst, String srcBucket, String srcKey, String dstBucket, String dstKey,
                                            long contentLength ) throws Exception {
    logger.info( "S3→S3 multipart copy initiated for large file: {} ({} bytes)", src.getQualifiedName(), contentLength );
    String uploadId = initiateMultipartUpload( dst, dstBucket, dstKey );
    List<PartETag> partETags = new ArrayList<>();
    ExecutorService executor = Executors.newFixedThreadPool( threadPoolSize );
    List<Future<PartETag>> futures = new ArrayList<>();
    try {
      submitCopyPartTasks( src, dst, srcBucket, srcKey, dstBucket, dstKey, uploadId, contentLength, executor, futures );
      for ( Future<PartETag> future : futures ) {
        partETags.add( future.get() );
      }
      executor.shutdown();
      completeMultipartUpload( dst, dstBucket, dstKey, uploadId, partETags );
    } catch ( Exception e ) {
      logger.error( "S3→S3 multipart copy failed, aborting upload: {} → {}: {}", src.getQualifiedName(), dst.getQualifiedName(), e.getMessage(), e );
      abortMultipartUpload( dst, dstBucket, dstKey, uploadId );
      executor.shutdownNow();
      throw e;
    }
  }

  private static String initiateMultipartUpload( S3FileObject dst, String dstBucket, String dstKey ) {
    InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest( dstBucket, dstKey );
    InitiateMultipartUploadResult initResponse = dst.fileSystem.getS3Client().initiateMultipartUpload( initRequest );
    return initResponse.getUploadId();
  }

  private void submitCopyPartTasks( S3FileObject src, S3FileObject dst, String srcBucket, String srcKey, String dstBucket, String dstKey,
                                           String uploadId, long contentLength, ExecutorService executor, List<Future<PartETag>> futures ) {
    long bytePosition = 0;
    int partNumber = 1;
    while ( bytePosition < contentLength ) {
      final long firstByte = bytePosition;
      final long lastByte = Math.min( bytePosition + partSize - 1, contentLength - 1 );
      final int thisPartNumber = partNumber;
      futures.add( executor.submit( () -> copyPart( dst, srcBucket, srcKey, dstBucket, dstKey, uploadId, firstByte, lastByte, thisPartNumber ) ) );
      bytePosition += partSize;
      partNumber++;
    }
  }

  private static PartETag copyPart( S3FileObject dst, String srcBucket, String srcKey, String dstBucket, String dstKey, String uploadId,
                                    long firstByte, long lastByte, int partNumber ) {
    CopyPartRequest copyPartRequest = new CopyPartRequest()
      .withSourceBucketName( srcBucket )
      .withSourceKey( srcKey )
      .withDestinationBucketName( dstBucket )
      .withDestinationKey( dstKey )
      .withFirstByte( firstByte )
      .withLastByte( lastByte )
      .withUploadId( uploadId )
      .withPartNumber( partNumber );
    CopyPartResult copyPartResult = dst.fileSystem.getS3Client().copyPart( copyPartRequest );
    logger.debug( "Copied part {} ({}-{})", partNumber, firstByte, lastByte );
    return copyPartResult.getPartETag();
  }

  private static void completeMultipartUpload( S3FileObject dst, String dstBucket, String dstKey, String uploadId,
                                               List<PartETag> partETags ) {
    CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest( dstBucket, dstKey, uploadId, partETags );
    dst.fileSystem.getS3Client().completeMultipartUpload( compRequest );
    logger.info( "S3→S3 multipart copy succeeded: {} → {}", dstBucket + "/" + dstKey, dst.getQualifiedName() );
  }

  private static void abortMultipartUpload( S3FileObject dst, String dstBucket, String dstKey, String uploadId ) {
    dst.fileSystem.getS3Client().abortMultipartUpload( new AbortMultipartUploadRequest( dstBucket, dstKey, uploadId ) );
  }
}
