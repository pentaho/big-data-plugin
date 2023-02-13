/*!
 * Copyright 2010 - 2022 Hitachi Vantara.  All rights reserved.
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

import org.apache.commons.vfs2.FileType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


/**
 * created by: rfellows date:       05/17/2012
 */
public class S3FileNameTest {

  public static final String SCHEME = "s3";
  public static final String SCHEME_DELIMITER = "://";

  private static final String BUCKET_ID = "FooBucket";

  @Test
  public void testGetURIWithoutBucket() {
    S3FileName fileName = new S3FileName( SCHEME, "", "", FileType.FOLDER );
    assertEquals( SCHEME + SCHEME_DELIMITER, fileName.getURI() );
  }

  @Test
  public void testGetURIWithBucket() {
    S3FileName fileName = new S3FileName( SCHEME, BUCKET_ID, "", FileType.FOLDER );
    assertEquals( SCHEME + SCHEME_DELIMITER, fileName.getURI() );
  }

  @Test
  public void testCreateNameWithoutBucket() {
    S3FileName fileName = new S3FileName( SCHEME, "", "", FileType.FOLDER );
    assertEquals( SCHEME + SCHEME_DELIMITER + "path/to/my/file",
      fileName.createName( "/path/to/my/file", FileType.FILE ).getURI() );
  }

  @Test
  public void testCreateNameWithBucket() {
    S3FileName fileName = new S3FileName( SCHEME, BUCKET_ID, "", FileType.FOLDER );
    assertEquals( SCHEME + SCHEME_DELIMITER + "path/to/my/file",
      fileName.createName( "/path/to/my/file", FileType.FILE ).getURI() );
  }

  @Test
  public void testAppendRootUriWithoutBucket() {
    S3FileName fileName = new S3FileName( SCHEME, "", "/FooFolder", FileType.FOLDER );
    assertEquals( SCHEME + SCHEME_DELIMITER + "FooFolder", fileName.getURI() );
  }

  @Test
  public void testAppendRootUriWithBucket() {
    S3FileName fileName = new S3FileName( SCHEME, BUCKET_ID, "/FooBucket/FooFolder", FileType.FOLDER );
    assertEquals( SCHEME + SCHEME_DELIMITER + "FooBucket/FooFolder", fileName.getURI() );
  }
}
