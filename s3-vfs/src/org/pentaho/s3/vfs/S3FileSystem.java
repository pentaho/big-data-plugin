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

import java.util.Collection;

import org.apache.commons.vfs.FileName;
import org.apache.commons.vfs.FileObject;
import org.apache.commons.vfs.FileSystem;
import org.apache.commons.vfs.FileSystemOptions;
import org.apache.commons.vfs.provider.AbstractFileSystem;
import org.apache.commons.vfs.provider.URLFileName;
import org.jets3t.service.S3Service;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.security.AWSCredentials;

public class S3FileSystem extends AbstractFileSystem implements FileSystem {

  private S3Service service;

  protected S3FileSystem(final FileName rootName, final FileSystemOptions fileSystemOptions) {
    super(rootName, null, fileSystemOptions);
  }

  @SuppressWarnings("unchecked")
  protected void addCapabilities(Collection caps) {
    caps.addAll(S3FileProvider.capabilities);
  }

  protected FileObject createFile(FileName name) throws Exception {
    return new S3FileObject(name, this);
  }

  public S3Service getS3Service() {
    if (service == null) {
      String awsAccessKey = ((URLFileName) getRootName()).getUserName();
      String awsSecretKey = ((URLFileName) getRootName()).getPassword();
      AWSCredentials awsCredentials = new AWSCredentials(awsAccessKey, awsSecretKey);
      try {
        service = new RestS3Service(awsCredentials);
      } catch (Throwable t) {
        System.out.println("Could not getS3Service() for " + awsCredentials);
        t.printStackTrace();
      }
    }
    return service;
  }

}
