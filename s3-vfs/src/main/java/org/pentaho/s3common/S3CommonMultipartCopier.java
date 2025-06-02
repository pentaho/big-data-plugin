package org.pentaho.s3common;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.vfs2.FileSystemException;
import org.pentaho.s3.vfs.S3FileObject;
import org.slf4j.Logger;

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
  static final long MULTIPART_COPY_THRESHOLD = 5L * 1024 * 1024 * 1024; // 5GB
  static final long MULTIPART_COPY_PART_SIZE = 100L * 1024 * 1024; // 100MB
  static final int DEFAULT_THREAD_POOL_SIZE = 8;

  /**
   * Perform S3→S3 multipart copy using AWS SDK. Uses multipart copy for files >5GB.
   * Supports multithreading for part copy operations.
   *
   * @param src    Source S3FileObject
   * @param dst    Destination S3FileObject
   * @param logger Logger for logging
   * @throws FileSystemException if copy fails
   */
  public static void multipartCopy( S3FileObject src, S3FileObject dst, Logger logger ) throws FileSystemException {
    multipartCopy( src, dst, logger, DEFAULT_THREAD_POOL_SIZE );
  }

  /**
   * Perform S3→S3 multipart copy with a custom thread pool size.
   *
   * @param src           Source S3FileObject
   * @param dst           Destination S3FileObject
   * @param logger        Logger for logging
   * @param threadPoolSize Number of threads for multipart copy
   * @throws FileSystemException if copy fails
   */
  public static void multipartCopy( S3FileObject src, S3FileObject dst, Logger logger, int threadPoolSize ) throws FileSystemException {
    String srcBucket = src.bucketName;
    String srcKey = src.key;
    String dstBucket = dst.bucketName;
    String dstKey = dst.key;
    try {
      ObjectMetadata srcMeta = getObjectMetadata( src, srcBucket, srcKey );
      long contentLength = srcMeta.getContentLength();
      if ( contentLength < MULTIPART_COPY_THRESHOLD ) {
        performSimpleCopy( src, dst, srcBucket, srcKey, dstBucket, dstKey, logger );
        return;
      }
      performMultipartCopy( src, dst, srcBucket, srcKey, dstBucket, dstKey, contentLength, logger, threadPoolSize );
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

  private static void performSimpleCopy( S3FileObject src, S3FileObject dst, String srcBucket, String srcKey, String dstBucket, String dstKey, Logger logger ) {
    CopyObjectRequest copyRequest = new CopyObjectRequest( srcBucket, srcKey, dstBucket, dstKey );
    dst.fileSystem.getS3Client().copyObject( copyRequest );
    logger.info( "S3→S3 server-side copy succeeded: {} → {}", src.getQualifiedName(), dst.getQualifiedName() );
  }

  private static void performMultipartCopy( S3FileObject src, S3FileObject dst, String srcBucket, String srcKey, String dstBucket, String dstKey, long contentLength, Logger logger, int threadPoolSize ) throws Exception {
    logger.info( "S3→S3 multipart copy initiated for large file: {} ({} bytes)", src.getQualifiedName(), contentLength );
    String uploadId = initiateMultipartUpload( dst, dstBucket, dstKey );
    List<PartETag> partETags = new ArrayList<>();
    ExecutorService executor = Executors.newFixedThreadPool( threadPoolSize );
    List<Future<PartETag>> futures = new ArrayList<>();
    try {
      submitCopyPartTasks( src, dst, srcBucket, srcKey, dstBucket, dstKey, uploadId, contentLength, executor, futures, logger );
      for ( Future<PartETag> future : futures ) {
        partETags.add( future.get() );
      }
      executor.shutdown();
      completeMultipartUpload( dst, dstBucket, dstKey, uploadId, partETags, logger );
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

  private static void submitCopyPartTasks( S3FileObject src, S3FileObject dst, String srcBucket, String srcKey, String dstBucket, String dstKey, String uploadId, long contentLength, ExecutorService executor, List<Future<PartETag>> futures, Logger logger ) {
    long bytePosition = 0;
    int partNumber = 1;
    while ( bytePosition < contentLength ) {
      final long firstByte = bytePosition;
      final long lastByte = Math.min( bytePosition + MULTIPART_COPY_PART_SIZE - 1, contentLength - 1 );
      final int thisPartNumber = partNumber;
      futures.add( executor.submit( () -> copyPart( dst, srcBucket, srcKey, dstBucket, dstKey, uploadId, firstByte, lastByte, thisPartNumber, logger ) ) );
      bytePosition += MULTIPART_COPY_PART_SIZE;
      partNumber++;
    }
  }

  private static PartETag copyPart( S3FileObject dst, String srcBucket, String srcKey, String dstBucket, String dstKey, String uploadId, long firstByte, long lastByte, int partNumber, Logger logger ) {
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

  private static void completeMultipartUpload( S3FileObject dst, String dstBucket, String dstKey, String uploadId, List<PartETag> partETags, Logger logger ) {
    CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest( dstBucket, dstKey, uploadId, partETags );
    dst.fileSystem.getS3Client().completeMultipartUpload( compRequest );
    logger.info( "S3→S3 multipart copy succeeded: {} → {}", dstBucket + "/" + dstKey, dst.getQualifiedName() );
  }

  private static void abortMultipartUpload( S3FileObject dst, String dstBucket, String dstKey, String uploadId ) {
    dst.fileSystem.getS3Client().abortMultipartUpload( new AbortMultipartUploadRequest( dstBucket, dstKey, uploadId ) );
  }
}
