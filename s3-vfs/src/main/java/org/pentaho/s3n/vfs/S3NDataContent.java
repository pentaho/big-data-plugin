/*!
* Copyright 2010 - 2018 Hitachi Vantara.  All rights reserved.
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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.APPEND;

public class S3NDataContent {
  private static final String TEMP_FILE_PREFIX = "s3vfs";
  private static final String S3VFS_USE_TEMPORARY_FILE_ON_UPLOAD_DATA = "s3.vfs.useTempFileOnUploadData";

  private boolean useTempFileOnUploadData;

  private Path dataFile;

  private OutputStream dataStream;

  private boolean isLoaded;

  public S3NDataContent() throws IOException {
    this.useTempFileOnUploadData = "Y".equals( ( System.getProperty( S3VFS_USE_TEMPORARY_FILE_ON_UPLOAD_DATA, "N" ) ) );
  }

  static Path createTempFile() throws IOException {
    Path tmpFile = Files.createTempFile( TEMP_FILE_PREFIX, null );
    tmpFile.toFile().deleteOnExit();
    return tmpFile;
  }

  public void load() throws IOException {
    if ( !isLoaded ) {
      OutputStream output = null;
      Path tmpFile = null;
      if ( useTempFileOnUploadData ) {
        tmpFile = createTempFile();
        output = Files.newOutputStream( tmpFile, APPEND );
      } else {
        output = new ByteArrayOutputStream();
      }
      this.dataFile = tmpFile;
      this.dataStream = output;
      isLoaded = true;
    }
  }

  public File asFile() {
    return dataFile != null ? dataFile.toFile() : null;
  }

  public ByteArrayOutputStream asByteArrayStream() {
    if ( this.dataStream instanceof ByteArrayOutputStream ) {
      return ( (ByteArrayOutputStream) this.dataStream );
    }
    return null;
  }

  public boolean isUseTempFileOnUploadData() {
    return useTempFileOnUploadData;
  }

  public OutputStream getDataToUpload() throws IOException {
    return this.dataStream;
  }

}
