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

public class S3CommonMultipartCopier {
  private static final long MULTIPART_COPY_THRESHOLD = 5L * 1024 * 1024 * 1024; // 5GB
  private static final long MULTIPART_COPY_PART_SIZE = 100L * 1024 * 1024; // 100MB
  private static final int DEFAULT_THREAD_POOL_SIZE = 8;

  /**
   * Perform S3→S3 multipart copy using AWS SDK. Uses multipart copy for files >5GB.
   * Supports multithreading for part copy operations.
   */
  public static void multipartCopy( S3FileObject src, S3FileObject dst, Logger logger ) throws FileSystemException {
    multipartCopy( src, dst, logger, DEFAULT_THREAD_POOL_SIZE );
  }

  public static void multipartCopy( S3FileObject src, S3FileObject dst, Logger logger, int threadPoolSize ) throws FileSystemException {
    String srcBucket = src.bucketName;
    String srcKey = src.key;
    String dstBucket = dst.bucketName;
    String dstKey = dst.key;
    try {
      ObjectMetadata srcMeta = src.fileSystem.getS3Client().getObjectMetadata( srcBucket, srcKey );
      long contentLength = srcMeta.getContentLength();
      if ( contentLength < MULTIPART_COPY_THRESHOLD ) {
        // Small file: use simple copy
        CopyObjectRequest copyRequest = new CopyObjectRequest( srcBucket, srcKey, dstBucket, dstKey );
        dst.fileSystem.getS3Client().copyObject( copyRequest );
        logger.info( "S3→S3 server-side copy succeeded: {} → {}", src.getQualifiedName(), dst.getQualifiedName() );
        return;
      }
      // Large file: multipart copy with multithreading
      logger.info( "S3→S3 multipart copy initiated for large file: {} ({} bytes)", src.getQualifiedName(), contentLength );
      InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest( dstBucket, dstKey );
      InitiateMultipartUploadResult initResponse = dst.fileSystem.getS3Client().initiateMultipartUpload( initRequest );
      String uploadId = initResponse.getUploadId();
      List<PartETag> partETags = new ArrayList<>();
      long bytePosition = 0;
      int partNumber = 1;

      ExecutorService executor = Executors.newFixedThreadPool( threadPoolSize );
      List<Future<PartETag>> futures = new ArrayList<>();
      try {
        while ( bytePosition < contentLength ) {
          final long firstByte = bytePosition;
          final long lastByte = Math.min( bytePosition + MULTIPART_COPY_PART_SIZE - 1, contentLength - 1 );
          final int thisPartNumber = partNumber;
          futures.add( executor.submit( () -> {
            CopyPartRequest copyPartRequest = new CopyPartRequest()
              .withSourceBucketName( srcBucket )
              .withSourceKey( srcKey )
              .withDestinationBucketName( dstBucket )
              .withDestinationKey( dstKey )
              .withFirstByte( firstByte )
              .withLastByte( lastByte )
              .withUploadId( uploadId )
              .withPartNumber( thisPartNumber );
            CopyPartResult copyPartResult = dst.fileSystem.getS3Client().copyPart( copyPartRequest );
            logger.debug( "Copied part {} ({}-{})", thisPartNumber, firstByte, lastByte );
            return copyPartResult.getPartETag();
          } ) );
          bytePosition += MULTIPART_COPY_PART_SIZE;
          partNumber++;
        }
        // Wait for all parts to finish
        for ( Future<PartETag> future : futures ) {
          partETags.add(  future.get() );
        }
        executor.shutdown();
        // 2. Complete multipart upload
        CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(
          dstBucket, dstKey, uploadId, partETags );
        dst.fileSystem.getS3Client().completeMultipartUpload( compRequest );
        logger.info( "S3→S3 multipart copy succeeded: {} → {}", src.getQualifiedName( ), dst.getQualifiedName() );
      } catch ( Exception e ) {
        logger.error( "S3→S3 multipart copy failed, aborting upload: {} → {}: {}", src.getQualifiedName(), dst.getQualifiedName(), e.getMessage(), e );
        dst.fileSystem.getS3Client().abortMultipartUpload(
          new AbortMultipartUploadRequest( dstBucket, dstKey, uploadId ) );
        executor.shutdownNow();
        throw e;
      }
    } catch ( AmazonS3Exception e ) {
      logger.error( "S3→S3 server-side copy failed: {} → {}: {}", src.getQualifiedName(), dst.getQualifiedName(), e.getMessage(), e );
      throw new FileSystemException( "vfs.provider.s3/copy-server-side.error", src.getQualifiedName(), dst.getQualifiedName(), e );
    } catch ( Exception e ) {
      logger.error( "Unexpected error during S3→S3 server-side copy: {} → {}: {}", src.getQualifiedName(), dst.getQualifiedName(), e.getMessage(), e );
      throw new FileSystemException( "vfs.provider.s3/copy-server-side.error", src.getQualifiedName(), dst.getQualifiedName(), e );
    }
  }
}
