/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.s3common;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.vfs2.FileSystemException;
import org.pentaho.s3.vfs.S3FileObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyPartRequest;
import com.amazonaws.services.s3.model.CopyPartResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;

public class S3CommonMultipartCopier {

  private static final String VFS_PROVIDER_S3_COPY_SERVER_SIDE_ERROR = "vfs.provider.s3/copy-server-side.error";

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
    try {
      ObjectMetadata srcMeta = getObjectMetadata( src );

      long contentLength = srcMeta.getContentLength();
      if ( contentLength < partSize ) {
        performSimpleCopy( src, dst );
      } else {
        performMultipartCopy( src, dst, contentLength );
      }
    } catch ( Exception e ) {
      throw new FileSystemException( VFS_PROVIDER_S3_COPY_SERVER_SIDE_ERROR, src.getQualifiedName(), dst.getQualifiedName(), e );
    }
  }

  private static ObjectMetadata getObjectMetadata( S3FileObject src ) {
    return src.fileSystem.getS3Client().getObjectMetadata( src.bucketName, src.key );
  }

  private static void performSimpleCopy( S3FileObject src, S3FileObject dst ) {
    CopyObjectRequest copyRequest = new CopyObjectRequest( src.bucketName, src.key, dst.bucketName, dst.key );
    dst.fileSystem.getS3Client().copyObject( copyRequest );
    logger.info( "S3→S3 server-side copy succeeded: {} → {}", src.getQualifiedName(), dst.getQualifiedName() );
  }

  private void performMultipartCopy( S3FileObject src, S3FileObject dst, long contentLength ) throws FileSystemException {
    logger.info( "S3→S3 multipart copy initiated for large file: {} ({} bytes)", src.getQualifiedName(), contentLength );
    String uploadId = initiateMultipartUpload( dst );
    List<PartETag> partETags = new ArrayList<>();
    ExecutorService executor = Executors.newFixedThreadPool( threadPoolSize );
    List<Future<PartETag>> futures = new ArrayList<>();
    try {
      submitCopyPartTasks( src, dst, uploadId, contentLength, executor, futures );
      collectPartETags( futures, partETags );
      executor.shutdown();
      completeMultipartUpload( dst, uploadId, partETags );
    } catch ( InterruptedException ie ) {
      abortMultipartUpload( dst, uploadId );
      executor.shutdownNow();
      Thread.currentThread().interrupt();
      throw new FileSystemException( VFS_PROVIDER_S3_COPY_SERVER_SIDE_ERROR, src.getQualifiedName(), dst.getQualifiedName(), ie );
    } catch ( ExecutionException e ) {
      abortMultipartUpload( dst, uploadId );
      executor.shutdownNow();
      throw new FileSystemException( VFS_PROVIDER_S3_COPY_SERVER_SIDE_ERROR, src.getQualifiedName(), dst.getQualifiedName(), e );
    }
  }

  private void collectPartETags( List<Future<PartETag>> futures, List<PartETag> partETags ) throws InterruptedException, ExecutionException {
    for ( Future<PartETag> future : futures ) {
      partETags.add( future.get() );
    }
  }

  private static String initiateMultipartUpload( S3FileObject dst ) {
    InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest( dst.bucketName, dst.key );
    InitiateMultipartUploadResult initResponse = dst.fileSystem.getS3Client().initiateMultipartUpload( initRequest );
    return initResponse.getUploadId();
  }

  private void submitCopyPartTasks( S3FileObject src, S3FileObject dst, String uploadId, long contentLength,
                                    ExecutorService executor, List<Future<PartETag>> futures ) {
    long bytePosition = 0;
    int partNumber = 1;
    while ( bytePosition < contentLength ) {
      final long firstByte = bytePosition;
      final long lastByte = Math.min( bytePosition + partSize - 1, contentLength - 1 );
      final int thisPartNumber = partNumber;
      futures.add( executor.submit( () -> copyPart( src, dst, uploadId, firstByte, lastByte, thisPartNumber ) ) );
      bytePosition += partSize;
      partNumber++;
    }
  }

  private static PartETag copyPart( S3FileObject src, S3FileObject dst, String uploadId,
                                    long firstByte, long lastByte, int partNumber ) {
    CopyPartRequest copyPartRequest = new CopyPartRequest()
      .withSourceBucketName( src.bucketName )
      .withSourceKey( src.key )
      .withDestinationBucketName( dst.bucketName )
      .withDestinationKey( dst.key )
      .withFirstByte( firstByte )
      .withLastByte( lastByte )
      .withUploadId( uploadId )
      .withPartNumber( partNumber );
    CopyPartResult copyPartResult = dst.fileSystem.getS3Client().copyPart( copyPartRequest );
    logger.debug( "Copied part {} ({}-{})", partNumber, firstByte, lastByte );
    return copyPartResult.getPartETag();
  }

  private static void completeMultipartUpload( S3FileObject dst, String uploadId,
                                               List<PartETag> partETags ) {
    CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest( dst.bucketName, dst.key, uploadId, partETags );
    dst.fileSystem.getS3Client().completeMultipartUpload( compRequest );
    logger.info( "S3→S3 multipart copy succeeded: {}/{} → {}", dst.bucketName, dst.key, dst.getQualifiedName() );
  }

  private static void abortMultipartUpload( S3FileObject dst, String uploadId ) {
    dst.fileSystem.getS3Client().abortMultipartUpload( new AbortMultipartUploadRequest( dst.bucketName, dst.key, uploadId ) );
  }
}
