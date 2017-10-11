/*!
* Copyright 2010 - 2017 Hitachi Vantara.  All rights reserved.
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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileObject;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;

public class S3FileObject extends AbstractFileObject {

  public static final String DELIMITER = "/";

  //  private S3Service service = null;
  private S3Bucket bucket = null;
  private S3FileSystem fileSystem = null;

  protected S3FileObject( final AbstractFileName name, final S3FileSystem fileSystem ) throws FileSystemException {
    super( name, fileSystem );
    this.fileSystem = fileSystem;
    //    service = fileSystem.getS3Service();
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

  protected S3Bucket getS3Bucket() throws Exception {
    if ( bucket == null ) {
      String bucketName = getS3BucketName();
      // subtract out the name
      S3Service s3Service = fileSystem.getS3Service();
      if ( s3Service != null ) {
        ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
        try {
          Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
          bucket = s3Service.getBucket( bucketName );
        } finally {
          Thread.currentThread().setContextClassLoader( currentClassLoader );
        }
      } else {
        return null;
      }

    }
    return bucket;
  }

  S3Object getS3Object( boolean deleteIfAlreadyExists, boolean needContent ) throws Exception {
    return getS3Object( deleteIfAlreadyExists, needContent, null );
  }

  S3Object getS3Object( boolean deleteIfAlreadyExists, boolean needContent, S3DataContent s3DataContent ) throws Exception {
    try {
      if ( getName().getPath().indexOf( DELIMITER, 1 ) == -1 ) {
        return null;
      }
      String name = getBucketRelativeS3Path();
      //      S3Object[] children = s3ChildrenMap.get(getS3BucketName());
      //      for (S3Object child : children) {
      //        if (child.getKey().equals(name)) {
      //          return child;
      //        }
      //      }

      if ( !name.equals( "" ) ) {
        try {
          S3Object object = getObjectFromS3( name, needContent );
          if ( deleteIfAlreadyExists ) {
            fileSystem.getS3Service().deleteObject( getS3BucketName(), name );
            object = getS3Object( name, s3DataContent );
          }
          return object;
        } catch ( Exception e ) {
          S3Object object = getS3Object( name, s3DataContent );
          if ( deleteIfAlreadyExists ) {
            fileSystem.getS3Service().deleteObject( getS3Bucket(), name );
          }
          return object;
        }
      }
    } catch ( Exception ex ) {
      //ignored
    }
    return null;
  }

  static S3Object getS3Object( String name, S3DataContent s3DataContent ) throws NoSuchAlgorithmException, IOException {
    S3Object object = new S3Object( name );
    if ( s3DataContent != null ) {
      if ( s3DataContent.isUseTempFileOnUploadData() ) {
        object = new S3Object( s3DataContent.asFile() );
        object.setName( name );
      } else {
        object = new S3Object( name, s3DataContent.asByteArrayStream().toByteArray() );
      }
    }
    return object;
  }

  private S3Object getObjectFromS3( String name, Boolean needContent ) throws S3ServiceException, IOException {
    S3Object s3Object = fileSystem.getS3Service().getObject( getS3BucketName(), name );
    if ( !needContent && s3Object != null ) {
      s3Object.closeDataInputStream();
    }
    return s3Object;
  }

  protected long doGetContentSize() throws Exception {
    return getS3Object( false, false ).getContentLength();
  }

  protected OutputStream doGetOutputStream( final boolean append ) throws Exception {
    S3DataContent s3DataContent = new S3DataContent();
    s3DataContent.load();
    final OutputStream output = s3DataContent.getDataToUpload();
    final PipedInputStream pis = new PipedInputStream();

    final Thread t = new Thread( new Runnable() {
      public void run() {
        try {
          IOUtils.copy( pis, output );
        } catch ( IOException e ) {
          e.printStackTrace();
        }
      }
    } );
    t.start();

    final PipedOutputStream pos = new PipedOutputStream() {
      public void close() throws IOException {
        super.close();
        try {
          // wait for reader to finish
          t.join();
          // get the s3 object to put it into S3 storage
          S3Object s3Object = getS3Object( true, false, s3DataContent );
          fileSystem.getS3Service().putObject( getS3Bucket(), s3Object );
        } catch ( Exception e ) {
          e.printStackTrace();
        } finally {
          output.close();
        }
      }
    };
    pis.connect( pos );

    return pos;
  }

  protected InputStream doGetInputStream() throws Exception {
    return getS3Object( false, true ).getDataInputStream();
  }

  protected FileType doGetType() throws Exception {
    S3Bucket bucket = null;
    try {
      bucket = getS3Bucket();
    } catch ( Exception ex ) {
      // ignored
    }
    if ( getName().getPath().equals( "" ) || getName().getPath().equals( DELIMITER ) || getName().getPath()
      .endsWith( DELIMITER ) ) {
      return FileType.FOLDER;
    }
    String s3Path = getBucketRelativeS3Path();
    if ( s3Path.isEmpty() && bucket != null ) {
      return FileType.FOLDER;
    }
    if ( !s3Path.endsWith( DELIMITER ) ) {
      s3Path = s3Path.concat( DELIMITER );
    }
    S3Object objectEndsWithDelimiter = null;
    try {
      objectEndsWithDelimiter = getObjectFromS3( s3Path, false );
    } catch ( Exception e ) {
      try {
        if ( fileSystem.getS3Service().listObjects( getS3BucketName(), s3Path, null ).length != 0 ) {
          return FileType.FOLDER;
        }
      } catch ( S3ServiceException se ) {
       // ignored
      }
    }
    if ( objectEndsWithDelimiter != null ) {
      return FileType.FOLDER;
    }
    S3Object object = null;
    try {
      object = getS3Object( false, false );
    } catch ( Exception ex ) {
      // ignored
    }

    if ( bucket == null && object == null ) {
      return FileType.IMAGINARY;
    } else if ( bucket != null && object == null ) {
      return FileType.FOLDER;
    } else if ( object.getBucketName() != null && object.getLastModifiedDate() != null ) {
      return FileType.FILE;
    }
    return FileType.IMAGINARY;
  }

  public void doCreateFolder() throws Exception {
    S3Object s3Object = getS3Object( false, false );
    if ( s3Object == null ) {
      bucket = fileSystem.getS3Service().getOrCreateBucket( getS3BucketName() );
    } else {
      // create fake folder
      bucket = fileSystem.getS3Service().getOrCreateBucket( getS3BucketName() );
      String name = getBucketRelativeS3Path() + DELIMITER;
      if ( name.equals( DELIMITER ) ) {
        return;
      }

      S3Object obj = new S3Object( bucket, name );
      fileSystem.getS3Service().putObject( bucket, obj );

      ( (S3FileObject) getParent() ).folders.add( getName().getBaseName() );
      s3ChildrenMap.remove( getS3BucketName() );

      // throw new FileSystemException("vfs.provider/create-folder-not-supported.error");
    }
  }

  public boolean canRenameTo( FileObject newfile ) {
    try {
      // we cannot rename buckets
      if ( getType().equals( FileType.FOLDER ) ) {
        return false;
      }
    } catch ( Exception e ) {
      //ignored
    }
    return super.canRenameTo( newfile );
  }

  public void doDelete() throws Exception {
    S3Object s3obj = getS3Object( false, false );
    bucket = getS3Bucket();
    if ( s3obj == null ) {     // If the selected object is null, getName() will cause exception.
      if ( bucket != null ) {  // Therefore, take care of the delete bucket case, first.
        fileSystem.getS3Service().deleteBucket( getS3BucketName() );
      }
      return;
    }
    if ( getName().getPath().equals( "" ) || getName().getPath().equals( DELIMITER ) ) {
      return;
    }

    String key = s3obj.getKey();
    FileType filetype = getName().getType();
    if ( filetype.equals( FileType.FILE ) ) {
      fileSystem.getS3Service().deleteObject( bucket, key );          // Delete a file.
    } else if ( filetype.equals( FileType.FOLDER ) ) {
      key = key + DELIMITER;                            // Delete a folder.
      fileSystem.getS3Service().deleteObject( bucket,
        key );          // The folder will not get deleted if its key does not end with DELIMITER.
    } else {
      return;
    }
    ( (S3FileObject) getParent() ).folders.remove( getName().getBaseName() );
    s3ChildrenMap.remove( getS3BucketName() );
  }

  protected void doRename( FileObject newfile ) throws Exception {
    if ( getType().equals( FileType.FOLDER ) ) {
      throw new FileSystemException( "vfs.provider/rename-not-supported.error" );
    }
    S3Object s3Object = getS3Object( false, false );
    if ( s3Object == null ) {
      throw new FileSystemException( "vfs.provider/rename.error", new Object[] { this, newfile } );
    }
    s3Object.setKey( newfile.getName().getBaseName() );
    fileSystem.getS3Service().renameObject( getS3BucketName(), getName().getBaseName(), s3Object );
    s3ChildrenMap.remove( getS3BucketName() );
  }

  protected long doGetLastModifiedTime() throws Exception {
    if ( getType() == FileType.FOLDER ) {
      return -1;
    }
    return getS3Object( false, false ).getLastModifiedDate().getTime();
  }

  protected boolean doSetLastModifiedTime( long modtime ) throws Exception {
    return true;
  }

  protected Set<String> folders = new HashSet<String>();
  protected static Map<String, S3Object[]> s3ChildrenMap = new HashMap<String, S3Object[]>();

  protected String[] doListChildren() throws Exception {
    S3Bucket bucket = getS3Bucket();
    if ( bucket == null && ( getName().getPath().equals( "" ) || getName().getPath().equals( DELIMITER ) ) ) {
      S3Bucket[] buckets = fileSystem.getS3Service().listAllBuckets();
      String[] children = new String[ buckets.length ];
      for ( int i = 0; i < buckets.length; i++ ) {
        children[ i ] = buckets[ i ].getName();
      }
      return children;
    } else {
      if ( s3ChildrenMap.get( getS3BucketName() ) == null ) {
        s3ChildrenMap.put( getS3BucketName(), fileSystem.getS3Service().listObjects( getS3BucketName() ) );
      }
      String s3Path = getBucketRelativeS3Path();
      S3Object[] s3Children = fileSystem.getS3Service().listObjects( getS3BucketName(), s3Path + DELIMITER, null );
      Set<String> vfsChildren = new HashSet<String>();
      if ( s3Children != null && !"".equals( s3Path ) ) {
        // let's see what we have in folders
        for ( S3Object obj : s3Children ) {
          String key = obj.getKey();
          String pathSegment = key.substring( s3Path.length() + 1 );
          int slashIndex = pathSegment.indexOf( DELIMITER );
          if ( slashIndex > 0 ) {
            String child = pathSegment.substring( 0, slashIndex );
            vfsChildren.add( child );
            folders.add( child );
          } else if ( !"".equalsIgnoreCase( pathSegment ) ) {
            vfsChildren.add( pathSegment );
          }
        }

      } else {
        s3Children = s3ChildrenMap.get( getS3BucketName() );
        if ( s3Children == null ) {
          return null;
        }
        for ( S3Object aS3Children : s3Children ) {
          String key = aS3Children.getKey();
          int slashIndex = key.indexOf( DELIMITER );
          if ( slashIndex > 0 ) {
            String child = key.substring( 0, slashIndex );
            vfsChildren.add( child );
            folders.add( child );
          } else {
            vfsChildren.add( key );
          }
        }
      }
      return vfsChildren.toArray( new String[] { } );
    }
  }

  private String getBucketRelativeS3Path() {
    if ( getName().getPath().indexOf( DELIMITER, 1 ) >= 0 ) {
      return getName().getPath().substring( getName().getPath().indexOf( DELIMITER, 1 ) + 1 );
    } else {
      return "";
    }
  }

  @Override protected void handleCreate( FileType newType ) throws Exception {
    s3ChildrenMap.remove( getS3BucketName() );
    super.handleCreate( newType );
  }

  @Override protected void handleDelete() throws Exception {
    s3ChildrenMap.remove( getS3BucketName() );
    super.handleDelete();
  }
}
