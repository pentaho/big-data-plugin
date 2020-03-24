/*!
* Copyright 2010 - 2020 Hitachi Vantara.  All rights reserved.
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
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import static junit.framework.Assert.assertEquals;

/**
 * created by: rfellows date:       05/17/2012
 */
public class S3FileNameTest {

  private S3FileName fileName = null;
  private static final String SCHEME_DELIMITER = ":/";

  public static final String awsAccessKey = "ABC123456DEF7890";             // fake out a key
  public static final String awsSecretKey = "A+123456BCD99/99999999ZZZ+B";   // fake out a secret key

  public static final String HOST = "S3";
  public static final String SCHEME = "s3";
  public static final int PORT = 843;

  @BeforeClass
  public static void init() throws Exception {
  }

  @Before
  public void setup() {
    fileName = new S3FileName( SCHEME, "/", "", FileType.FOLDER );
  }

  @Test
  public void testGetURI() throws Exception {
    String expected = buildS3URL( "/" );
    assertEquals( expected, fileName.getURI() );
  }


  @Test
  public void testCreateName() throws Exception {
    assertEquals( "s3://path/to/my/file",
            fileName.createName( "/path/to/my/file", FileType.FILE ).getURI() );
  }

  @Test
  public void testAppendRootUriWithNonDefaultPort() throws Exception {
    fileName = new S3FileName( SCHEME, "/", "FooFolder", FileType.FOLDER );
    String expectedUri = SCHEME + SCHEME_DELIMITER + "FooFolder";
    assertEquals( expectedUri, fileName.getURI() );

    fileName = new S3FileName( SCHEME, "FooBucket", "FooBucket/FooFolder", FileType.FOLDER );
    expectedUri = SCHEME + ":/FooBucket/" + "FooFolder";
    assertEquals( expectedUri, fileName.getURI() );
  }

  public static String buildS3URL( String path ) throws UnsupportedEncodingException {
    return SCHEME + SCHEME_DELIMITER + path;
  }

}
