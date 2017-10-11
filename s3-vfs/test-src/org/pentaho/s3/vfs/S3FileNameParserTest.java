/*!
* Copyright 2010 - 2017 Hitachi Vantara.  All rights reserved.
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

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.provider.FileNameParser;
import org.apache.commons.vfs2.provider.VfsComponentContext;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;


/**
 * Unit tests for S3FileNameParser
 */
public class S3FileNameParserTest {

  FileNameParser parser;

  @Before
  public void setUp() throws Exception {
    parser = S3FileNameParser.getInstance();
  }

  @Test
  public void testParseUri() throws Exception {
    VfsComponentContext context = mock( VfsComponentContext.class );
    FileName fileName = mock( FileName.class );
    String uri = "s3://hostname:8080/bucket";
    FileName noBaseFile = parser.parseUri( context, null, uri );
    assertNotNull( noBaseFile );
    FileName withBaseFile = parser.parseUri( context, fileName, uri );
    assertNotNull( withBaseFile );

  }

  @Test
  public void testEncodeAccessKeys() throws Exception {
    String fullUrl = "s3://ABC123456DEF7890:A+123456BCD99/99999999ZZZ+B@S3hostname:8080/bucket";
    String encodedUrl = ( (S3FileNameParser) parser ).encodeAccessKeys( fullUrl );
    assertEquals( "s3://ABC123456DEF7890:A%2B123456BCD99%2F99999999ZZZ%2BB@S3hostname:8080/bucket", encodedUrl );
  }
}
