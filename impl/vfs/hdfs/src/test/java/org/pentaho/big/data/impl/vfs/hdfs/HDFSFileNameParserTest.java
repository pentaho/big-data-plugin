/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 * <p>
 * ******************************************************************************
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 ******************************************************************************/

package org.pentaho.big.data.impl.vfs.hdfs;

import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.provider.URLFileName;
import org.apache.commons.vfs2.provider.URLFileNameParser;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

/**
 * Created by bryan on 8/7/15.
 */
public class HDFSFileNameParserTest {

  @Rule public final ExpectedException exception = ExpectedException.none();

  @Test
  public void testDefaultPort() {
    assertEquals( -1, HDFSFileNameParser.getInstance().getDefaultPort() );
  }

  @Test
  public void testParseUriNullInput() throws FileSystemException {
    HDFSFileNameParser.getInstance().parseUri( null, null, "testfs://test" );
  }

  @Test
  public void testParseUriMixedCase() throws FileSystemException {
    URLFileName urlFileName =
      (URLFileName) HDFSFileNameParser.getInstance().parseUri( null, null, "hdfs://testUpperCaseHost" );
    assertEquals( "testUpperCaseHost", urlFileName.getHostName() );
  }

  @Test
  public void testParseUriMixedCaseLongName() throws FileSystemException {
    URLFileName urlFileName =
      (URLFileName) HDFSFileNameParser.getInstance().parseUri( null, null, "hdfs://testUpperCaseHost/long/test/name" );
    assertEquals( "testUpperCaseHost", urlFileName.getHostName() );
  }

  @Test
  public void testParseUriThrowExceptionNoProtocol() throws FileSystemException {
    exception.expect( FileSystemException.class );
    HDFSFileNameParser.getInstance().parseUri( null, null, "testUpperCaseHost/long/test/name" );
  }

  @Test
  public void testParseUriUserNameFilePath() throws FileSystemException {
    String filename = "hdfs://root:password@testUpperCaseHost:8080/long/test/name";
    URLFileName hdfsFileName =
      (URLFileName) HDFSFileNameParser.getInstance()
        .parseUri( null, null, filename );
    URLFileName urlFileName = (URLFileName) new URLFileNameParser( 7000 ).parseUri( null, null, filename );
    assertEquals( 8080, hdfsFileName.getPort() );
    assertEquals( "root", hdfsFileName.getUserName() );
    assertEquals( "/long/test/name", hdfsFileName.getPath() );
    assertEquals( "password", hdfsFileName.getPassword() );
    assertEquals( urlFileName.getType(), hdfsFileName.getType() );
    assertEquals( urlFileName.getQueryString(), hdfsFileName.getQueryString() );
  }
}
