/*!
* Copyright 2010 - 2017 Pentaho Corporation.  All rights reserved.
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Path;

import org.junit.Before;
import org.junit.Test;

public class S3DataContentTest {
  private static final String S3VFS_USE_TEMPORARY_FILE_ON_UPLOAD_DATA = "s3.vfs.useTempFileOnUploadData";
  private S3DataContent s3DataContent;

  @Before
  public void setUp() {
    s3DataContent = null;
  }

  @Test
  public void testS3DataContent_NotUseTemporaryFileOnUpload() throws IOException {
    turnOffUseTemporaryFileOnUploadData();
    s3DataContent = new S3DataContent();
    assertNotNull( s3DataContent );
    assertFalse( s3DataContent.isUseTempFileOnUploadData() );
    assertNull( s3DataContent.asFile() );
    assertNull( s3DataContent.asByteArrayStream() );
  }

  @Test
  public void testS3DataContent_UseTemporaryFileOnUpload() throws IOException {
    turnOnUseTemporaryFileOnUploadData();
    s3DataContent = new S3DataContent();
    assertNotNull( s3DataContent );
    assertTrue( s3DataContent.isUseTempFileOnUploadData() );
    assertNull( s3DataContent.asFile() );
    assertNull( s3DataContent.asByteArrayStream() );
  }

  @Test
  public void testLoad_UseTemporaryFileOnUpload() throws IOException {
    turnOnUseTemporaryFileOnUploadData();
    s3DataContent = new S3DataContent();
    s3DataContent.load();
    assertNotNull( s3DataContent.getDataToUpload() );
    assertNotNull( s3DataContent.asFile() );
    assertNull( s3DataContent.asByteArrayStream() );
  }

  @Test
  public void testLoad_NotUseTemporaryFileOnUpload() throws IOException {
    turnOffUseTemporaryFileOnUploadData();
    s3DataContent = new S3DataContent();
    s3DataContent.load();
    assertNotNull( s3DataContent.getDataToUpload() );
    assertNull( s3DataContent.asFile() );
    assertNotNull( s3DataContent.asByteArrayStream() );
  }

  @Test
  public void testCreateTempFile_UseTemporaryFileOnUpload() throws IOException {
    turnOnUseTemporaryFileOnUploadData();
    Path createTempFile = S3DataContent.createTempFile();
    assertTrue( createTempFile.toFile().exists() );
    assertTrue( createTempFile.toFile().getName().startsWith( "s3vfs" ) );
  }

  private static void turnOnUseTemporaryFileOnUploadData() {
    System.getProperties().put( S3VFS_USE_TEMPORARY_FILE_ON_UPLOAD_DATA, "Y" );
  }

  private static void turnOffUseTemporaryFileOnUploadData() {
    System.getProperties().put( S3VFS_USE_TEMPORARY_FILE_ON_UPLOAD_DATA, "N" );
  }

}
