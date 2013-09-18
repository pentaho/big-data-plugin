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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs.FileName;
import org.apache.commons.vfs.FileObject;
import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.FileType;
import org.apache.commons.vfs.provider.AbstractFileObject;
import org.jets3t.service.S3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;

public class S3FileObject extends AbstractFileObject implements FileObject {

//  private S3Service service = null;
  private S3Bucket bucket = null;
  private S3FileSystem fileSystem = null;

  protected S3FileObject(final FileName name, final S3FileSystem fileSystem) throws FileSystemException {
    super(name, fileSystem);
    this.fileSystem = fileSystem;
//    service = fileSystem.getS3Service();
  }

  protected String getS3BucketName() throws Exception {
    String bucketName = getName().getPath();
    if (bucketName.indexOf("/", 1) > 1) {
      // this file is a file, to get the bucket, remove the name from the path
      bucketName = bucketName.substring(1, bucketName.indexOf("/", 1));
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
      bucket = fileSystem.getS3Service().getBucket(bucketName);
    }
    return bucket;
  }

  protected S3Object getS3Object(boolean deleteIfAlreadyExists) throws Exception {
    try {
      if (getName().getPath().indexOf("/", 1) == -1) {
        return null;
      }
      String name = getName().getPath().substring(getName().getPath().indexOf("/", 1) + 1);
      
//      S3Object[] children = s3ChildrenMap.get(getS3BucketName());
//      for (S3Object child : children) {
//        if (child.getKey().equals(name)) {
//          return child;
//        }
//      }
      
      if (!name.equals("")) {
        try {
          S3Object object = fileSystem.getS3Service().getObject(getS3Bucket(), name);
          if (deleteIfAlreadyExists) {
            bucket = getS3Bucket();
            bucket = fileSystem.getS3Service().createBucket(getS3BucketName());
            fileSystem.getS3Service().deleteObject(getS3Bucket(), name);
            object = new S3Object(name);
          }
          return object;
        } catch (Exception e) {
          S3Object object = new S3Object(name);
          if (deleteIfAlreadyExists) {
            bucket = getS3Bucket();
            bucket = fileSystem.getS3Service().createBucket(getS3BucketName());
            fileSystem.getS3Service().deleteObject(getS3Bucket(), name);
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
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final PipedInputStream pis = new PipedInputStream();

    final Thread t = new Thread(new Runnable() {
      public void run() {
        try {
          IOUtils.copy(pis, output);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    });
    t.start();

    final PipedOutputStream pos = new PipedOutputStream() {
      public void close() throws IOException {
        super.close();
        try {
          // wait for reader to finish
          t.join();
          S3Object s3Object = getS3Object(true);
          byte[] bytes = output.toByteArray();
          s3Object.setContentLength(bytes.length);
          s3Object.setDataInputStream(new ByteArrayInputStream(bytes));
          fileSystem.getS3Service().putObject(getS3Bucket(), s3Object);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    };
    pis.connect(pos);

    return pos;
  }

  public void close() throws FileSystemException {
    try {
      getS3Object(false).closeDataInputStream();
      super.close();
    } catch (Exception e) {
    }
  }

  protected InputStream doGetInputStream() throws Exception {
    return getS3Object(false).getDataInputStream();
  }

  protected FileType doGetType() throws Exception {

    if (getName().getPath().equals("") || getName().getPath().equals("/")) {
      return FileType.FOLDER;
    }

    S3FileObject parent = (S3FileObject) getParent();
    if (parent.folders == null || parent.folders.isEmpty()) {
      // Refresh the list of children from our parent so we can determine if we're a folder or not
      // TODO This should be cleaned up so we don't have to fetch all children. Ideally within #getS3Object().
      parent.doListChildren();
    }
    if (((S3FileObject) getParent()).folders.contains(getName().getBaseName())) {
      return FileType.FOLDER;
    }
    
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

    if (bucket == null && object == null) {
      return FileType.IMAGINARY;
    } else if (bucket != null && object == null) {
      return FileType.FOLDER;
    } else if (object.getBucketName() != null && object.getLastModifiedDate() != null) {
      return FileType.FILE;
    }
    return FileType.IMAGINARY;
  }

  public void doCreateFolder() throws Exception {
    if (getS3Object(false) == null) {
      bucket = fileSystem.getS3Service().createBucket(getS3BucketName());
    } else {
      // create fake folder
      bucket = fileSystem.getS3Service().createBucket(getS3BucketName());
      String name = getName().getPath().substring(getName().getPath().indexOf("/", 1) + 1) + "/";

      S3Object obj = new S3Object(bucket, name);
      fileSystem.getS3Service().putObject(bucket, obj);

      ((S3FileObject) getParent()).folders.add(getName().getBaseName());
      s3ChildrenMap.remove(getS3BucketName());

      // throw new FileSystemException("vfs.provider/create-folder-not-supported.error");
    }
  }

  public boolean canRenameTo(FileObject newfile) {
    try {
      // we cannot rename buckets
      if (getType().equals(FileType.FOLDER)) {
        return false;
      }
    } catch (Exception e) {
    }
    return super.canRenameTo(newfile);
  }

  public void doDelete() throws Exception {
    S3Object s3obj = getS3Object(false);
    bucket = getS3Bucket();
    if (s3obj == null) {     // If the selected object is null, getName() will cause exception. 
      if (bucket != null) {  // Therefore, take care of the delete bucket case, first.
        fileSystem.getS3Service().deleteBucket(bucket);
      }  
      return;
    }
    
    if (getName().getPath().equals("") || getName().getPath().equals("/")) {
      return;
    }

    String key = s3obj.getKey();
    FileType filetype = getName().getType();
    if (filetype.equals(FileType.FILE)) {
      fileSystem.getS3Service().deleteObject(bucket, key);          // Delete a file.
    } else if (filetype.equals(FileType.FOLDER)) {
      key = key + "/";                            // Delete a folder.
      fileSystem.getS3Service().deleteObject(bucket, key);          // The folder will not get deleted if its key does not end with "/".
    } else {
      return;
    }
    ((S3FileObject) getParent()).folders.remove(getName().getBaseName());
    s3ChildrenMap.remove(getS3BucketName());
  }

  protected void doRename(FileObject newfile) throws Exception {
    if (getType().equals(FileType.FOLDER)) {
      throw new FileSystemException("vfs.provider/rename-not-supported.error");
    }
    S3Object s3Object = getS3Object(false);
    s3Object.setKey(newfile.getName().getBaseName());
    fileSystem.getS3Service().renameObject(getS3BucketName(), getName().getBaseName(), s3Object);
  }

  protected long doGetLastModifiedTime() throws Exception {
    if (getType() == FileType.FOLDER) {
      return -1;
    }
    return getS3Object(false).getLastModifiedDate().getTime();
  }

  protected void doSetLastModifiedTime(long modtime) throws Exception {
  }

  protected Set<String> folders = new HashSet<String>();
  protected static Map<String, S3Object[]> s3ChildrenMap = new HashMap<String, S3Object[]>();

  protected String[] doListChildren() throws Exception {
    S3Bucket bucket = getS3Bucket();
    if (bucket == null && (getName().getPath().equals("") || getName().getPath().equals("/"))) {
      S3Bucket[] buckets = fileSystem.getS3Service().listAllBuckets();
      String[] children = new String[buckets.length];
      for (int i = 0; i < buckets.length; i++) {
        children[i] = buckets[i].getName();
      }
      return children;
    } else {
      if (s3ChildrenMap.get(getS3BucketName()) == null) {
        s3ChildrenMap.put(getS3BucketName(), fileSystem.getS3Service().listObjects(getS3Bucket()));
      }
      S3Object[] s3Children = s3ChildrenMap.get(getS3BucketName());
      Set<String> vfsChildren = new HashSet<String>();

      if (s3Children != null && getName().getPath().indexOf("/", 1) >= 0) {
        String s3Path = getName().getPath().substring(getName().getPath().indexOf("/", 1) + 1);
        // let's see what we have in folders
        for (S3Object obj : s3Children) {
          String key = obj.getKey();
          if (key.startsWith(s3Path)) {
            // go from end of key match to next slash or end of key, whichever comes first
            String pathSegment = key.substring(s3Path.length() + 1);
            int slashIndex = pathSegment.indexOf("/");
            if (slashIndex > 0) {
              String child = pathSegment.substring(0, slashIndex);
              vfsChildren.add(child);
              folders.add(child);
            } else if (!"".equalsIgnoreCase(pathSegment) && pathSegment != null) {
              vfsChildren.add(pathSegment);
            }
          }
        }
      } else {
        for (int i = 0; i < s3Children.length; i++) {
          String key = s3Children[i].getKey();
          int slashIndex = key.indexOf("/");
          if (slashIndex > 0) {
            String child = key.substring(0, slashIndex);
            vfsChildren.add(child);
            folders.add(child);
          } else {
            vfsChildren.add(key);
          }
        }
      }
      return vfsChildren.toArray(new String[] {});
    }
  }

}
