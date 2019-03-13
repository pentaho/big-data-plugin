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

package org.pentaho.s3n.vfs;

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
import java.util.List;

public class S3NFileObject extends AbstractFileObject {

  private static final Logger logger = LoggerFactory.getLogger( S3NFileObject.class );
  public static final String DELIMITER = "/";
  private S3NFileSystem fileSystem;
  private String bucketName;
  private String key;
  private S3Object s3Object;

  protected S3NFileObject( final AbstractFileName name, final S3NFileSystem fileSystem ) throws FileSystemException {
    super( name, fileSystem );
    this.fileSystem = fileSystem;
    this.bucketName = getS3BucketName();
    this.key = getBucketRelativeS3Path();
  }

  @Override
  protected long doGetContentSize() {
    return getS3Object().getObjectMetadata().getContentLength();
  }

  @Override protected InputStream doGetInputStream() throws Exception {
    logger.debug( "Accessing content " + getQualifiedName() );
    activateContent();
    return s3Object.getObjectContent();
  }

  @Override protected FileType doGetType() throws Exception {
    return getType();
  }

  @Override protected String[] doListChildren() throws Exception {
    List<String> childrenList = new ArrayList<String>();

    // only listing folders or the root bucket
    if ( getType() == FileType.FOLDER || isRootBucket() ) {
      childrenList = getS3ObjectsFromVirtualFolder();
    }
    String[] childrenArr = new String[ childrenList.size() ];

    return childrenList.toArray( childrenArr );
  }

  protected String getS3BucketName() {
    String bucketName = getName().getPath();
    if ( bucketName.indexOf( DELIMITER, 1 ) > 1 ) {
      // this file is a file, to get the bucket, remove the name from the path
      bucketName = bucketName.substring( 1, bucketName.indexOf( DELIMITER, 1 ) );
    } else {
      // this file is a bucket
      bucketName = bucketName.replaceAll( DELIMITER, "" );
    }
    return bucketName;
  }

  private List<String> getS3ObjectsFromVirtualFolder() {
    List<String> childrenList = new ArrayList<String>();

    // fix cases where the path doesn't include the final delimiter
    String realKey = key;
    if ( !realKey.endsWith( DELIMITER ) ) {
      realKey += DELIMITER;
    }

    if ( "".equals( key ) && "".equals( bucketName ) ) {
      //Getting buckets in root folder
      List<Bucket> bucketList = fileSystem.getS3Client().listBuckets();
      for ( Bucket bucket : bucketList ) {
        childrenList.add( bucket.getName() + "/" );
      }
    } else {
      //Getting files/folders in a folder/bucket
      ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
        .withBucketName( bucketName )
        .withPrefix( key )
        .withDelimiter( DELIMITER );

      ObjectListing ol = fileSystem.getS3Client().listObjects( listObjectsRequest );

      ArrayList<S3ObjectSummary> allSummaries = new ArrayList<S3ObjectSummary>( ol.getObjectSummaries() );
      ArrayList<String> allCommonPrefixes = new ArrayList<String>( ol.getCommonPrefixes() );

      // get full list
      while ( ol.isTruncated() ) {
        ol = fileSystem.getS3Client().listNextBatchOfObjects( ol );
        allSummaries.addAll( ol.getObjectSummaries() );
        allCommonPrefixes.addAll( ol.getCommonPrefixes() );
      }

      for ( S3ObjectSummary s3os : allSummaries ) {
        if ( !s3os.getKey().equals( realKey ) ) {
          childrenList.add( s3os.getKey().substring( key.length() ) );
        }
      }

      for ( String commonPrefix : allCommonPrefixes ) {
        if ( !commonPrefix.equals( realKey ) ) {
          childrenList.add( commonPrefix.substring( key.length() ) );
        }
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

  @VisibleForTesting
  S3Object getS3Object() {
    return getS3Object( this.key );
  }

  private S3Object getS3Object( String key ) {
    if ( s3Object != null && s3Object.getObjectContent() != null ) {
      logger.debug( "Returning exisiting object " + getQualifiedName() );
      return s3Object;
    } else {
      logger.debug( "Getting object " + getQualifiedName() );
      return fileSystem.getS3Client().getObject( bucketName, key );
    }
  }

  @VisibleForTesting
  S3Object activateContent() throws IOException {
    if ( s3Object != null ) {
      // force it to re-create the object
      s3Object.close();
      s3Object = null;
    }

    s3Object = getS3Object();
    return s3Object;
  }

  private boolean isRootBucket() {
    return key.equals( "" );
  }

  @Override protected void doAttach() throws Exception {
    logger.debug( "Attach called on " + getQualifiedName() );
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

  protected void doDetach() throws Exception {
    if ( s3Object != null ) {
      logger.debug( "detaching " + getQualifiedName() );
      this.getS3Object().close();
    }
  }

  @Override
  public void doDelete() throws FileSystemException {

    // can only delete folder if empty
    if ( getType() == FileType.FOLDER ) {

      // list all children inside the folder
      ObjectListing ol = fileSystem.getS3Client().listObjects( bucketName, key );
      ArrayList<S3ObjectSummary> allSummaries = new ArrayList<S3ObjectSummary>( ol.getObjectSummaries() );

      // get full list
      while ( ol.isTruncated() ) {
        ol = fileSystem.getS3Client().listNextBatchOfObjects( ol );
        allSummaries.addAll( ol.getObjectSummaries() );
      }

      for ( S3ObjectSummary s3os : allSummaries ) {
        fileSystem.getS3Client().deleteObject( bucketName, s3os.getKey() );
      }
    }

    fileSystem.getS3Client().deleteObject( bucketName, key );
  }

  @Override
  protected OutputStream doGetOutputStream( boolean bAppend ) throws Exception {
    return new S3NPipedOutputStream( this.fileSystem, bucketName, key );
  }

  @Override protected long doGetLastModifiedTime() throws Exception {
    return s3Object.getObjectMetadata().getLastModified().getTime();
  }

  @Override protected void doCreateFolder() throws Exception {
    if ( !isRootBucket() ) {
      // create meta-data for your folder and set content-length to 0
      ObjectMetadata metadata = new ObjectMetadata();
      metadata.setContentLength( 0 );
      metadata.setContentType( "binary/octet-stream" );

      // create empty content
      InputStream emptyContent = new ByteArrayInputStream( new byte[ 0 ] );

      // create a PutObjectRequest passing the folder name suffixed by /
      PutObjectRequest putObjectRequest = new PutObjectRequest( bucketName, key + DELIMITER, emptyContent, metadata );

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

  @Override protected void doRename( FileObject newFile ) throws Exception {

    // no folder renames on S3
    if ( getType().equals( FileType.FOLDER ) ) {
      throw new FileSystemException( "vfs.provider/rename-not-supported.error" );
    }

    if ( s3Object == null ) {
      // object doesn't exist
      throw new FileSystemException( "vfs.provider/rename.error", new Object[] { this, newFile } );
    }

    S3NFileObject dest = (S3NFileObject) newFile;

    // 1. copy the file
    CopyObjectRequest copyObjRequest = new CopyObjectRequest( bucketName, key, dest.bucketName, dest.key );
    fileSystem.getS3Client().copyObject( copyObjRequest );

    // 2. delete self
    delete();
  }

  private String getQualifiedName() {
    return getQualifiedName( this );
  }

  private String getQualifiedName( S3NFileObject s3nFileObject ) {
    return s3nFileObject.bucketName + "/" + s3nFileObject.key;
  }

}
