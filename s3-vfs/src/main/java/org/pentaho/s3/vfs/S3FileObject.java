/*!
 * Copyright 2010 - 2019 Hitachi Vantara.  All rights reserved.
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

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.AbstractMap.SimpleEntry;

public class S3FileObject extends AbstractFileObject {

  private static final Logger logger = LoggerFactory.getLogger( S3FileObject.class );
  public static final String DELIMITER = "/";
  private S3FileSystem fileSystem;
  private String bucketName;
  private String key;
  private S3Object s3Object;

  protected S3FileObject( final AbstractFileName name, final S3FileSystem fileSystem ) {
    super( name, fileSystem );
    this.fileSystem = fileSystem;
    this.bucketName = getS3BucketName();
    this.key = getBucketRelativeS3Path();
  }

  @Override protected long doGetContentSize() throws Exception {
    S3Object object = fileSystem.getS3Client().getObject( bucketName, key );
    return object.getObjectMetadata().getContentLength();
  }

  @Override protected InputStream doGetInputStream() throws Exception {
    logger.debug( "Accessing content {}", getQualifiedName() );
    activateContent();
    return s3Object.getObjectContent();
  }

  @Override protected FileType doGetType() throws Exception {
    return getType();
  }

  @Override protected String[] doListChildren() throws Exception {
    List<String> childrenList = new ArrayList<>();

    // only listing folders or the root bucket
    if ( getType() == FileType.FOLDER || isRootBucket() ) {
      childrenList = getS3ObjectsFromVirtualFolder();
    }
    String[] childrenArr = new String[ childrenList.size() ];

    return childrenList.toArray( childrenArr );
  }

  @Override
  public FileObject[] getChildren() throws FileSystemException {
    FileObject[] children = super.getChildren();
    // Must close all the input streams for the children or they will fill up the open http request resource pool
    // and degrade performance
    for ( FileObject child : children ) {
      S3FileObject o = (S3FileObject) child;
      if ( o.key != null && !o.key.equals( "" ) && child.getType() == FileType.FILE ) {
        try {
          logger.debug( "Closing inputStream {}", getQualifiedName( o ) );
          o.getS3Object().getObjectContent().close();
        } catch ( IOException e ) {
          logger.debug( "Caught exception ", e );
        }
      }
    }
    return children;
  }

  protected String getS3BucketName() {
    String s3BucketName = getName().getPath();
    if ( s3BucketName.indexOf( DELIMITER, 1 ) > 1 ) {
      // this file is a file, to get the bucket, remove the name from the path
      s3BucketName = s3BucketName.substring( 0, s3BucketName.indexOf( DELIMITER, 1 ) );
    } else {
      // this file is a bucket
      s3BucketName = s3BucketName.replaceAll( DELIMITER, "" );
    }
    return s3BucketName;
  }

  private List<String> getS3ObjectsFromVirtualFolder() {
    List<String> childrenList = new ArrayList<>();

    //see if bucket name needs to be adjusted from old driver pattern
    SimpleEntry<String, String> newPath = fixFilePath( key, bucketName );

    // fix cases where the path doesn't include the final delimiter
    String realKey = newPath.getKey();
    if ( !realKey.endsWith( DELIMITER ) ) {
      realKey += DELIMITER;
    }

    if ( "".equals( newPath.getKey() ) && "".equals( newPath.getValue() ) ) {
      //Getting buckets in root folder
      List<Bucket> bucketList = fileSystem.getS3Client().listBuckets();
      for ( Bucket bucket : bucketList ) {
        childrenList.add( bucket.getName() + "/" );
      }
    } else {
      childrenList = listChildFiles( newPath, realKey );
    }
    return childrenList;
  }

  private List<String> listChildFiles( SimpleEntry<String, String> pathPair, String realKey ) {
    List<String> childrenList = new ArrayList<>();
    //Getting files/folders in a folder/bucket
    String prefix = pathPair.getKey().isEmpty() || pathPair.getKey().endsWith( DELIMITER ) ? pathPair.getKey() : pathPair.getKey() + DELIMITER;
    ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
      .withBucketName( pathPair.getValue() )
      .withPrefix( prefix )
      .withDelimiter( DELIMITER );

    ObjectListing ol = fileSystem.getS3Client().listObjects( listObjectsRequest );

    ArrayList<S3ObjectSummary> allSummaries = new ArrayList<>( ol.getObjectSummaries() );
    ArrayList<String> allCommonPrefixes = new ArrayList<>( ol.getCommonPrefixes() );

    // get full list
    while ( ol.isTruncated() ) {
      ol = fileSystem.getS3Client().listNextBatchOfObjects( ol );
      allSummaries.addAll( ol.getObjectSummaries() );
      allCommonPrefixes.addAll( ol.getCommonPrefixes() );
    }

    for ( S3ObjectSummary s3os : allSummaries ) {
      if ( !s3os.getKey().equals( realKey ) ) {
        childrenList.add( s3os.getKey().substring( prefix.length() ) );
      }
    }

    for ( String commonPrefix : allCommonPrefixes ) {
      if ( !commonPrefix.equals( realKey ) ) {
        childrenList.add( commonPrefix.substring( prefix.length() ) );
      }
    }
    return childrenList;
  }

  private String getBucketRelativeS3Path() {
    if ( getName().getPath().indexOf( DELIMITER, 1 ) >= 0 ) {
      return getName().getPath().substring( getName().getPath().indexOf( DELIMITER, 1 ) + 1 );
    } else {
      return "";
    }
  }

  protected S3Object getS3Object() {
    return getS3Object( this.key );
  }

  private S3Object getS3Object( String key ) {
    if ( s3Object != null && s3Object.getObjectContent() != null ) {
      logger.debug( "Returning existing object {}", getQualifiedName() );
      return s3Object;
    } else {
      logger.debug( "Getting object {}", getQualifiedName() );
      SimpleEntry<String, String> newPath = fixFilePath( key, bucketName );
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

  private S3Object activateContent() {
    s3Object = null; //Force it to re-create the object
    s3Object = getS3Object();
    return s3Object;
  }

  private boolean isRootBucket() {
    SimpleEntry<String, String> newPath = fixFilePath( key, bucketName );
    return newPath.getKey().equals( "" );
  }

  @Override
  protected void doAttach() throws Exception {
    logger.debug( "Attach called on {}", getQualifiedName() );
    injectType( FileType.IMAGINARY );

    if ( isRootBucket() ) {
      // cannot attach to root bucket
      injectType( FileType.FOLDER );
      return;
    }

    // 1. Is it an existing file?
    try {
      s3Object = getS3Object();
      injectType( getName().getType() ); // if this worked then the automatically detected type is right

    } catch ( AmazonS3Exception e ) {
      // S3 object doesn't exist

      // 2. Is it in reality a folder?
      String keyWithDelimiter = key + DELIMITER;
      try {
        s3Object = getS3Object( keyWithDelimiter );
        injectType( FileType.FOLDER );
        this.key = keyWithDelimiter;
      } catch ( AmazonS3Exception e2 ) {
        String[] splittedKeys = keyWithDelimiter.split( DELIMITER );
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
          .withBucketName( splittedKeys[0] )
          .withPrefix( getPrefixFromKeys( splittedKeys ) )
          .withDelimiter( DELIMITER );
        ObjectListing ol = fileSystem.getS3Client().listObjects( listObjectsRequest );

        if ( ol.getCommonPrefixes() .size() > 0 || ol.getObjectSummaries().size() > 0 ) {
          injectType( FileType.FOLDER );
        } else {
          //Folders don't really exist - they will generate a "NoSuckKey" exception
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
  }

  @Override
  protected void doDetach() throws Exception {
    if ( s3Object != null ) {
      logger.debug( "detaching {}", getQualifiedName() );
      this.getS3Object().close();
    }
  }

  @Override
  public void doDelete() throws FileSystemException {

    //see if bucket name needs to be adjusted from old driver pattern
    SimpleEntry<String, String> newPath = fixFilePath( key, bucketName );

    // can only delete folder if empty
    if ( getType() == FileType.FOLDER ) {

      // list all children inside the folder
      ObjectListing ol = fileSystem.getS3Client().listObjects( newPath.getValue(), newPath.getKey() );
      ArrayList<S3ObjectSummary> allSummaries = new ArrayList<>( ol.getObjectSummaries() );

      // get full list
      while ( ol.isTruncated() ) {
        ol = fileSystem.getS3Client().listNextBatchOfObjects( ol );
        allSummaries.addAll( ol.getObjectSummaries() );
      }

      for ( S3ObjectSummary s3os : allSummaries ) {
        fileSystem.getS3Client().deleteObject( newPath.getValue(), s3os.getKey() );
      }
    }

    fileSystem.getS3Client().deleteObject( newPath.getValue(), newPath.getKey() );
  }

  @Override
  protected OutputStream doGetOutputStream( boolean bAppend ) throws Exception {
    SimpleEntry<String, String> newPath = fixFilePath( key, bucketName );
    return new S3PipedOutputStream( this.fileSystem, newPath.getValue(), newPath.getKey() );
  }

  @Override
  protected long doGetLastModifiedTime() throws Exception {
    return s3Object.getObjectMetadata().getLastModified().getTime();
  }

  @Override
  protected void doCreateFolder() throws Exception {
    if ( !isRootBucket() ) {
      // create meta-data for your folder and set content-length to 0
      ObjectMetadata metadata = new ObjectMetadata();
      metadata.setContentLength( 0 );
      metadata.setContentType( "binary/octet-stream" );

      // create empty content
      InputStream emptyContent = new ByteArrayInputStream( new byte[ 0 ] );

      // create a PutObjectRequest passing the folder name suffixed by /
      SimpleEntry<String, String> newPath = fixFilePath( key, bucketName );

      PutObjectRequest putObjectRequest =
        new PutObjectRequest( newPath.getValue(), newPath.getKey() + DELIMITER, emptyContent, metadata );

      // send request to S3 to create folder
      try {
        fileSystem.getS3Client().putObject( putObjectRequest );
      } catch ( AmazonS3Exception e ) {
        throw new FileSystemException( "vfs.provider.local/create-folder.error", this, e );
      }
    } else {
      throw new FileSystemException( "vfs.provider/create-folder-not-supported.error" );
    }
  }

  @Override
  protected void doRename( FileObject newFile ) throws Exception {

    // no folder renames on S3
    if ( getType().equals( FileType.FOLDER ) ) {
      throw new FileSystemException( "vfs.provider/rename-not-supported.error" );
    }

    if ( s3Object == null ) {
      // object doesn't exist
      throw new FileSystemException( "vfs.provider/rename.error", this, newFile );
    }

    S3FileObject dest = (S3FileObject) newFile;

    // 1. copy the file
    SimpleEntry<String, String> newSourcePath = fixFilePath( key, bucketName );
    // Assumption: top-level buckets cannot be created here, so the new bucket must already exist or this should
    // throw an error.
    SimpleEntry<String, String> newDestPath = fixFilePath( dest.key, dest.bucketName );

    CopyObjectRequest copyObjRequest = new CopyObjectRequest( newSourcePath.getValue(), newSourcePath.getKey(),
      newDestPath.getValue(), newDestPath.getKey() );
    fileSystem.getS3Client().copyObject( copyObjRequest );

    // 2. delete self
    delete();
  }

  private String getQualifiedName() {
    return getQualifiedName( this );
  }

  private String getQualifiedName( S3FileObject s3FileObject ) {
    //only used for debugging messages; will skip translating any leading s3: bucket from the path
    return s3FileObject.bucketName + "/" + s3FileObject.key;
  }

  // Check if the given bucket name starts with s3:.  If so, the bucket name is probably an artifact of the old S3 file
  // system implementation and no part of the real path.  Return an adjusted bucket and key pair for use in talking to
  // the S3 service.
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

  @VisibleForTesting
  String getPrefixFromKeys( String[] keys ) {
    if ( keys == null || keys.length <= 1 ) {
      return "";
    }

    return Arrays.stream( keys )
      .skip( 1 ) // skip the first element which is the bucket name
      .collect( Collectors.joining( DELIMITER ) ) + DELIMITER; // join all other elements and end with a trailing /
  }
}
