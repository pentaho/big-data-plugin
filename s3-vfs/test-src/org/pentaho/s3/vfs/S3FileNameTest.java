/*!
* Copyright 2010 - 2013 Pentaho Corporation.  All rights reserved.
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
    fileName = new S3FileName( SCHEME, HOST, PORT, PORT, awsAccessKey, awsSecretKey, "/", FileType.FOLDER, null );
  }

  @Test
  public void testGetURI() throws Exception {
    String expected = buildS3URL( "/", true );
    assertEquals( expected, fileName.getURI() );
  }

  @Test
  public void testGetFriendlyURI() throws Exception {
    // make sure the Access Key & Secret Key are not part of the friendly URI
    String expected = buildS3URL( "/", false );
    assertEquals( expected, fileName.getFriendlyURI() );
  }

  @Test
  public void testCreateName() throws Exception {
    fileName.createName( "/path/to/my/file", FileType.FILE );
  }

  @Test
  public void testAppendRootUriWithNonDefaultPort() throws Exception {
    fileName = new S3FileName( SCHEME, HOST, PORT + 1, PORT, awsAccessKey, awsSecretKey, "/", FileType.FOLDER, null );
    String expectedUri = SCHEME + "://" + URLEncoder.encode( awsAccessKey, "UTF-8" ) + ":"
      + URLEncoder.encode( awsSecretKey, "UTF-8" ) + "@" + HOST + ":" + ( PORT + 1 ) + "/";
    assertEquals( expectedUri, fileName.getURI() );
  }

  public static String buildS3URL( String path, boolean withUserInfo ) throws UnsupportedEncodingException {
    if ( withUserInfo ) {
      return SCHEME + "://" + URLEncoder.encode( awsAccessKey, "UTF-8" ) + ":" + URLEncoder
        .encode( awsSecretKey, "UTF-8" ) + "@" + HOST + path;
    } else {
      return SCHEME + "://" + HOST + path;
    }
  }

}
