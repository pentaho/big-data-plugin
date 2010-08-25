/*
 * Copyright 2010 Pentaho Corporation.  All rights reserved.
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
 * @author Michael D'Amour
 */
package org.pentaho.s3.vfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import org.apache.commons.vfs.FileName;
import org.apache.commons.vfs.FileObject;
import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.FileType;
import org.apache.commons.vfs.provider.AbstractFileObject;
import org.jets3t.service.S3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;

public class S3FileObject extends AbstractFileObject implements FileObject {

  private S3Service service = null;
  private S3Bucket bucket = null;

  protected S3FileObject(final FileName name, final S3FileSystem fileSystem) throws FileSystemException {
    super(name, fileSystem);
    service = fileSystem.getS3Service();
  }

  protected String getS3BucketName() throws Exception {
    String bucketName = getName().getPath();
    if (bucketName.indexOf("/", 1) > 1) {
      // this file is a file, to get the bucket, remove the name from the path
      bucketName = bucketName.substring(1, bucketName.indexOf(getName().getBaseName()) - 1);
    } else {
      // this file is a bucket
      bucketName = bucketName.replaceAll("/", "");
    }
    return bucketName;
  }

  protected S3Bucket getS3Bucket() throws Exception {
    if (bucket == null) {
      String bucketName = getS3BucketName();
      // subtract out the name
      bucket = service.getBucket(bucketName);
    }
    return bucket;
  }

  protected S3Object getS3Object(boolean deleteIfAlreadyExists) throws Exception {
    try {
      if (getName().getPath().indexOf("/", 1) == -1) {
        return null;
      }
      String name = getName().getPath().substring(getName().getPath().indexOf("/", 1) + 1);
      if (!name.equals("")) {
        try {
          S3Object object = service.getObject(getS3Bucket(), name);
          return object;
        } catch (Exception e) {
          S3Object object = new S3Object(name);
          if (deleteIfAlreadyExists) {
            bucket = getS3Bucket();
            bucket = service.createBucket(getS3BucketName());
            service.deleteObject(getS3Bucket(), name);
          }
          return object;
        }
      }
    } catch (Exception ex) {
      ex.printStackTrace();
    }
    return null;
  }

  protected long doGetContentSize() throws Exception {
    return getS3Object(false).getContentLength();
  }

  protected OutputStream doGetOutputStream(final boolean append) throws Exception {
    final PipedInputStream pis = new PipedInputStream();
    final PipedOutputStream pos = new PipedOutputStream() {
      public void close() throws IOException {
        super.close();
        try {
          S3Object s3Object = getS3Object(true);
          s3Object.setDataInputStream(pis);
          service.putObject(getS3Bucket(), s3Object);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    };
    pis.connect(pos);
    return pos;

    // final PipedInputStream pis = new PipedInputStream();
    // final PipedOutputStream pos = new PipedOutputStream();
    // pis.connect(pos);
    //
    // S3Object s3Object = getS3Object(true);
    // s3Object.setDataInputStream(pis);
    // service.putObject(getS3Bucket(), s3Object);
    //
    // return pos;

  }

  public void close() throws FileSystemException {
    super.close();
    try {
      System.out.println("closing " + getName().getURI());
      getS3Object(false).closeDataInputStream();
    } catch (Exception e) {
    }
  }

  protected InputStream doGetInputStream() throws Exception {
    return getS3Object(false).getDataInputStream();
  }

  protected FileType doGetType() throws Exception {
    S3Bucket bucket = null;
    S3Object object = null;
    try {
      bucket = getS3Bucket();
    } catch (Exception ex) {
    }
    try {
      object = getS3Object(false);
    } catch (Exception ex) {
    }

    if (getName().getPath().equals("") || getName().getPath().equals("/")) {
      return FileType.FOLDER;
    }

    if (bucket == null && object == null) {
      return FileType.IMAGINARY;
    } else if (bucket != null && object == null) {
      return FileType.FOLDER;
    } else {
      return FileType.FILE;
    }
  }

  public void doCreateFolder() throws Exception {
    bucket = service.createBucket(getS3BucketName());
  }

  public void doDelete() throws Exception {
    if (getName().getPath().equals("") || getName().getPath().equals("/")) {
      return;
    }
    if (getType() == FileType.FILE) {
      service.deleteObject(getS3Bucket(), getName().getBaseName());
    } else if (getType() == FileType.FOLDER) {
      service.deleteBucket(getS3Bucket());
    }
  }

  protected void doRename(FileObject newfile) throws Exception {
    service.renameObject(newfile.getName().getPath(), newfile.getName().getBaseName(), getS3Object(false));
  }

  protected long doGetLastModifiedTime() throws Exception {
    if (getType() == FileType.FOLDER) {
      return -1;
    }
    return getS3Object(false).getLastModifiedDate().getTime();
  }

  protected void doSetLastModifiedTime(long modtime) throws Exception {
  }

  protected String[] doListChildren() throws Exception {
    S3Bucket bucket = getS3Bucket();
    if (bucket == null && (getName().getPath().equals("") || getName().getPath().equals("/"))) {
      S3Bucket[] buckets = service.listAllBuckets();
      String[] children = new String[buckets.length];
      for (int i = 0; i < buckets.length; i++) {
        children[i] = buckets[i].getName();
      }
      return children;
    } else {
      S3Object[] s3Children = service.listObjects(getS3Bucket());
      String[] children = new String[s3Children.length];
      for (int i = 0; i < s3Children.length; i++) {
        children[i] = s3Children[i].getKey();
      }
      return children;
    }
  }

}
