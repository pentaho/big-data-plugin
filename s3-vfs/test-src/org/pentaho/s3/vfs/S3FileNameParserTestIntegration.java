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

import org.apache.commons.vfs.FileName;
import org.apache.commons.vfs.provider.FileNameParser;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Properties;

import static junit.framework.Assert.assertEquals;

/**
 * created by: rfellows date:       5/25/12
 */
public class S3FileNameParserTestIntegration {

  public static String awsAccessKey;
  public static String awsSecretKey;

  public static final String HOST = "s3";
  public static final String SCHEME = "s3";
  public static final int PORT = 843;

  @BeforeClass
  public static void init() throws Exception {
    Properties settings = new Properties();
    settings.load( S3FileUtilTestIntegration.class.getResourceAsStream( "/test-settings.properties" ) );
    awsAccessKey = settings.getProperty( "awsAccessKey" );
    awsSecretKey = settings.getProperty( "awsSecretKey" );
  }

  @Test
  public void testParseUri_withKeys() throws Exception {
    FileNameParser parser = S3FileNameParser.getInstance();
    String expected = buildS3URL( "/rcf-emr-staging", true );

    FileName filename =
      parser.parseUri( null, null, "s3://" + awsAccessKey + ":" + awsSecretKey + "@" + HOST + "/rcf-emr-staging" );
    assertEquals( expected, filename.getURI() );

  }

  @Test
  public void testParseUri_withoutKeys() throws Exception {
    FileNameParser parser = S3FileNameParser.getInstance();
    String expected = buildS3URL( "/", false );

    FileName filename = parser.parseUri( null, null, "s3://" + HOST + "/" );
    assertEquals( expected, filename.getURI() );

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
