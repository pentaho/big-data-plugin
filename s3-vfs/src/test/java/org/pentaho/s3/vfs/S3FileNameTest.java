/*!
* Copyright 2010 - 2021 Hitachi Vantara.  All rights reserved.
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
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


/**
 * created by: rfellows date:       05/17/2012
 */
public class S3FileNameTest {

  private S3FileName fileName = null;

  private static final String BUCKET_ID = "bucketId";
  private static final String SCHEME_DELIMITER = ":/";

  public static final String SCHEME = "s3";

  @Before
  public void setup() {
    fileName = new S3FileName( SCHEME, BUCKET_ID, "", FileType.FOLDER );
  }

  @Test
  public void testGetURI() {
    String expected = buildS3URL( "bucketId/" );
    assertEquals( expected, fileName.getURI() );
  }


  @Test
  public void testCreateName() {
    assertEquals( "s3:/bucketId/path/to/my/file",
      fileName.createName( "/path/to/my/file", FileType.FILE ).getURI() );
  }

  @Test
  public void testAppendRootUriWithNonDefaultPort() {
    String fooFolder = "FooFolder";
    String fooBucket = "FooBucket";
    fileName = new S3FileName( SCHEME, BUCKET_ID, fooFolder, FileType.FOLDER );
    String expectedUri = SCHEME + SCHEME_DELIMITER + BUCKET_ID + fooFolder;
    assertEquals( expectedUri, fileName.getURI() );

    fileName = new S3FileName( SCHEME, fooBucket, fooFolder, FileType.FOLDER );
    expectedUri = SCHEME + SCHEME_DELIMITER + fooBucket + fooFolder;
    assertEquals( expectedUri, fileName.getURI() );
  }

  public static String buildS3URL( String path ) {
    return SCHEME + SCHEME_DELIMITER + path;
  }
}
