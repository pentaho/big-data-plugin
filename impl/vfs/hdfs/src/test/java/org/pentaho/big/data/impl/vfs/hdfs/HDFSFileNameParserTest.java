/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.apache.commons.vfs2.provider.URLFileName;
import org.apache.commons.vfs2.provider.URLFileNameParser;
import org.apache.commons.vfs2.provider.UriParser;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.doAnswer;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Created by bryan on 8/7/15.
 */
@RunWith( PowerMockRunner.class )
@PrepareForTest( { UriParser.class, VFS.class } )
public class HDFSFileNameParserTest {
  private static final String PREFIX = "hdfs";
  private static final String BASE_PATH = "//";
  private static final String BASE_URI = PREFIX + ":" + BASE_PATH;

  private StandardFileSystemManager fsm;

  @Rule public final ExpectedException exception = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    mockStatic( VFS.class );
    mockStatic( UriParser.class );
    spy( UriParser.class );

    fsm = mock( StandardFileSystemManager.class );
    when( VFS.getManager() ).thenReturn( fsm );
  }

  @Test
  public void testDefaultPort() {
    assertEquals( -1, HDFSFileNameParser.getInstance().getDefaultPort() );
  }

  @Test
  public void testParseUriNullInput() throws Exception {
    final String FILEPATH = "test";
    final String URI = BASE_URI + FILEPATH;

    buildExtractSchemeMocks( PREFIX, URI, BASE_PATH + FILEPATH );

    HDFSFileNameParser.getInstance().parseUri( null, null, URI );
  }

  @Test
  public void testParseUriMixedCase() throws Exception {
    final String FILEPATH = "testUpperCaseHost";
    final String URI = BASE_URI + FILEPATH;

    buildExtractSchemeMocks( PREFIX, URI, BASE_PATH + FILEPATH );

    URLFileName urlFileName =
      (URLFileName) HDFSFileNameParser.getInstance().parseUri( null, null, URI );
    assertEquals( "testUpperCaseHost", urlFileName.getHostName() );
  }

  @Test
  public void testParseUriMixedCaseLongName() throws Exception {
    final String FILEPATH = "testUpperCaseHost/long/test/name";
    final String URI = BASE_URI + FILEPATH;

    buildExtractSchemeMocks( PREFIX, URI, BASE_PATH + FILEPATH );

    URLFileName urlFileName =
      (URLFileName) HDFSFileNameParser.getInstance().parseUri( null, null, URI );
    assertEquals( "testUpperCaseHost", urlFileName.getHostName() );
  }

  @Test
  public void testParseUriThrowExceptionNoProtocol() throws Exception {
    final String FILEPATH = "testUpperCaseHost/long/test/name";
    exception.expect( FileSystemException.class );
    buildExtractSchemeMocks( null, FILEPATH, FILEPATH );
    HDFSFileNameParser.getInstance().parseUri( null, null, "testUpperCaseHost/long/test/name" );
  }

  @Test
  public void testParseUriUserNameFilePath() throws Exception {
    final String FILEPATH = "root:password@testUpperCaseHost:8080/long/test/name";
    final String URI = BASE_URI + FILEPATH;

    buildExtractSchemeMocks( PREFIX, URI, BASE_PATH + FILEPATH );

    URLFileName hdfsFileName =
      (URLFileName) HDFSFileNameParser.getInstance()
        .parseUri( null, null, URI );
    URLFileName urlFileName = (URLFileName) new URLFileNameParser( 7000 ).parseUri( null, null, URI );
    assertEquals( 8080, hdfsFileName.getPort() );
    assertEquals( "root", hdfsFileName.getUserName() );
    assertEquals( "/long/test/name", hdfsFileName.getPath() );
    assertEquals( "password", hdfsFileName.getPassword() );
    assertEquals( urlFileName.getType(), hdfsFileName.getType() );
    assertEquals( urlFileName.getQueryString(), hdfsFileName.getQueryString() );
  }

  private Answer buildSchemeAnswer( String prefix, String buildPath ) {
    Answer extractSchemeAnswer = invocation -> {
      Object[] args = invocation.getArguments();
      ( (StringBuilder) args[2] ).append( buildPath );
      return prefix;
    };
    return extractSchemeAnswer;
  }

  private void buildExtractSchemeMocks( String prefix, String fullPath, String pathWithoutPrefix ) throws Exception {
    String[] schemes = { "hdfs" };
    when( fsm.getSchemes() ).thenReturn( schemes );
    doAnswer( buildSchemeAnswer( prefix, pathWithoutPrefix ) ).when( UriParser.class, "extractScheme",
      eq( schemes ), eq( fullPath ), any( StringBuilder.class ) );
  }
}
