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


package org.pentaho.s3.vfs;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelector;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.pentaho.di.connections.vfs.provider.ResolvedConnectionFileObject;
import org.pentaho.s3common.S3CommonFileObject;
import org.pentaho.s3common.S3CommonPipedOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.AbstractMap.SimpleEntry;
import java.util.List;

import static java.util.AbstractMap.SimpleEntry;

public class S3FileObject extends S3CommonFileObject {

  private static final Logger logger = LoggerFactory.getLogger( S3FileObject.class );

  protected S3FileObject( final AbstractFileName name, final S3FileSystem fileSystem ) {
    super( name, fileSystem );
    //bucket name needs to be adjusted
    this.bucketName = getS3BucketName();
  }

  @Override
  protected String getS3BucketName() {
    String s3BucketName = getName().getPath();
    if ( s3BucketName.indexOf( DELIMITER, 1 ) > 1 ) {
      // this file is a file, to get the bucket, remove the name from the path
      //aws-sdk-version 1.12.460 or later requires bucket name without special character "/" -> https://github.com/aws/aws-sdk-java/discussions/2976
      s3BucketName = s3BucketName.substring( 1, s3BucketName.indexOf( DELIMITER, 1 ) );
    } else {
      // this file is a bucket
      s3BucketName = s3BucketName.replaceAll( DELIMITER, "" );
    }
    return s3BucketName;
  }

  @Override
  protected List<String> getS3ObjectsFromVirtualFolder( String key, String bucket ) {
    //see if bucket name needs to be adjusted from old driver pattern
    SimpleEntry<String, String> newPath = fixFilePath( key, bucket );

    return super.getS3ObjectsFromVirtualFolder( newPath.getKey(), newPath.getValue() );
  }

  @Override
  protected S3Object getS3Object( String key, String bucket ) {
    if ( s3Object != null && s3Object.getObjectContent() != null ) {
      logger.debug( "Returning existing object {}", getQualifiedName() );
      return s3Object;
    } else {
      logger.debug( "Getting object {}", getQualifiedName() );
      SimpleEntry<String, String> newPath = fixFilePath( key, bucket );
      return fileSystem.getS3Client().getObject( newPath.getValue(), newPath.getKey() );
    }
  }

  private boolean bucketExists( String bucket ) {
    boolean bucketExists = false;
    try {
      bucketExists = fileSystem.getS3Client().doesBucketExistV2( bucket );
    } catch ( SdkClientException e ) {
      logger.debug( "Exception checking if bucket exists", e );
    }
    return bucketExists;
  }

  @Override
  protected boolean isRootBucket() {
    SimpleEntry<String, String> newPath = fixFilePath( key, bucketName );
    return newPath.getKey().equals( "" );
  }

  @Override
  protected void doDelete() throws FileSystemException {
    //see if bucket name needs to be adjusted from old driver pattern
    SimpleEntry<String, String> newPath = fixFilePath( key, bucketName );

    doDelete( newPath.getKey(), newPath.getValue() );
  }

  @Override
  protected OutputStream doGetOutputStream( boolean bAppend ) throws IOException {
    SimpleEntry<String, String> newPath = fixFilePath( key, bucketName );
    S3FileSystem s3FileSystem = (S3FileSystem) this.fileSystem;
    return new S3CommonPipedOutputStream( this.fileSystem, newPath.getValue(), newPath.getKey(),
            s3FileSystem.getPartSize(),
            s3FileSystem.getThreadPoolSize() );
  }

  @Override
  protected PutObjectRequest createPutObjectRequest( String bucketName, String key, InputStream inputStream, ObjectMetadata objectMetadata ) {
    SimpleEntry<String, String> newPath = fixFilePath( key, bucketName );
    return new PutObjectRequest( newPath.getValue(), newPath.getKey(), inputStream, objectMetadata );
  }

  @Override
  protected CopyObjectRequest createCopyObjectRequest( String sourceBucket, String sourceKey, String destBucket, String destKey ) {
    SimpleEntry<String, String> sourcePath = fixFilePath( sourceKey, sourceBucket );
    SimpleEntry<String, String> destPath = fixFilePath( destKey, destBucket );
    return new CopyObjectRequest( sourcePath.getValue(), sourcePath.getKey(), destPath.getValue(), destPath.getKey() );
  }

  @Override
  protected void handleAttachException( String key, String bucket ) throws FileSystemException {
    SimpleEntry<String, String> newPath = fixFilePath( key, bucket );
    String keyWithDelimiter = newPath.getKey() + DELIMITER;
    try {
      s3Object = getS3Object( keyWithDelimiter, newPath.getValue() );
      s3ObjectMetadata = s3Object.getObjectMetadata();
      injectType( FileType.FOLDER );
    } catch ( AmazonS3Exception e2 ) {
      ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
              .withBucketName( newPath.getValue() )
              .withPrefix( keyWithDelimiter )
              .withDelimiter( DELIMITER );
      ObjectListing ol = fileSystem.getS3Client().listObjects( listObjectsRequest );

      if ( !( ol.getCommonPrefixes().isEmpty() && ol.getObjectSummaries().isEmpty() ) ) {
        injectType( FileType.FOLDER );
      } else {
        //Folders don't really exist - they will generate a "NoSuchKey" exception
        String errorCode = e2.getErrorCode();
        // confirms key doesn't exist but connection okay
        if ( !errorCode.equals( "NoSuchKey" ) ) {
          // bubbling up other connection errors
          logger.error( "Could not get information on " + getQualifiedName(),
                  e2 ); // make sure this gets printed for the user
          throw new FileSystemException( "vfs.provider/get-type.error", getQualifiedName(), e2 );
        }
      }
    }
  }

  // See if the first name on the path is actually a bucket.  If not, it's probably an old-style path.
  protected SimpleEntry<String, String> fixFilePath( String key, String bucket ) {
    String newBucket = bucket;
    String newKey = key;

    //see if the folder exists; if not, it might be from an old path and the real bucket is in the key
    if ( !bucketExists( bucket ) ) {
      logger.debug( "Bucket {} from original path not found, might be an old path from the old driver", bucket );
      if ( key.split( DELIMITER ).length > 1 ) {
        newBucket = key.split( DELIMITER )[0];
        newKey = key.replaceFirst( newBucket + DELIMITER, "" );
      } else {
        newBucket = key;
        newKey = "";
      }
    }
    return new SimpleEntry<>( newKey, newBucket );
  }

  /**
   * Attempts to extract an S3FileObject from a FileObject, including wrappers like ResolvedConnectionFileObject (via reflection for getResolvedFileObject()).
   */
  private S3FileObject extractDelegateS3FileObject(FileObject file) {
    if (file instanceof S3FileObject) {
      return (S3FileObject) file;
    }
    if ( file instanceof ResolvedConnectionFileObject) {
      // If it's a ResolvedConnectionFileObject, we can try to get the underlying S3FileObject
      ResolvedConnectionFileObject resolved = (ResolvedConnectionFileObject) file;
      FileObject delegate = resolved.getResolvedFileObject();
      if (delegate instanceof S3FileObject) {
        return (S3FileObject) delegate;
      }
    }
    return null;
  }

  @Override
  public void copyFrom(final FileObject file, final FileSelector selector ) throws FileSystemException {
    S3FileObject src = extractDelegateS3FileObject( file );
    if ( src == null ) {
      // Fallback to default implementation for non-S3 sources or wrappers
      super.copyFrom( file, selector );
      return;
    }
    S3FileObject dst = this;

    // Compare bucket names and region (if available) to determine if both are in the same S3 root
    String srcBucket = src.bucketName;
    String dstBucket = dst.bucketName;
    boolean sameBucket = srcBucket != null && srcBucket.equals( dstBucket );
    boolean sameRegion = true;
    try {
      String srcRegion = src.fileSystem.getS3Client().getBucketLocation( srcBucket );
      String dstRegion = dst.fileSystem.getS3Client().getBucketLocation( dstBucket );
      sameRegion = srcRegion != null && srcRegion.equals( dstRegion );
    } catch ( Exception e ) {
      logger.warn( "Could not determine S3 bucket region, proceeding with bucket name check only: {}", e.getMessage(), e );
    }

    if ( sameBucket && sameRegion ) {
      // Check if both source and destination objects exist and are files (not folders)
      try {
        if ( src.getType().hasContent() && dst.getType().hasContent() ) {
          logger.info( "Attempting S3→S3 server-side multipart copy from {} to {}", src.getQualifiedName(), dst.getQualifiedName() );
          org.pentaho.s3common.S3CommonMultipartCopier.multipartCopy(src, dst, logger);
          return;
        }
      } catch ( Exception e ) {
        logger.warn( "S3→S3 multipart copy failed, falling back to default copy: {}", e.getMessage(), e );
        // fallback to default
      }
    }
    // Fallback to default implementation
    super.copyFrom( file, selector );
  }
}
