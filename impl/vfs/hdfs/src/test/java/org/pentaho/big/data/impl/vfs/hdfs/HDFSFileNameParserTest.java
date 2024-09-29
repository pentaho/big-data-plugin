/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package org.pentaho.big.data.impl.vfs.hdfs;

import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.apache.commons.vfs2.provider.FileNameParser;
import org.apache.commons.vfs2.provider.URLFileName;
import org.apache.commons.vfs2.provider.URLFileNameParser;
import org.apache.commons.vfs2.provider.UriParser;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 8/7/15.
 */
@RunWith( MockitoJUnitRunner.class )
public class HDFSFileNameParserTest {
  private static final String PREFIX = "hdfs";
  private static final String BASE_PATH = "//";
  private static final String BASE_URI = PREFIX + ":" + BASE_PATH;

  private StandardFileSystemManager fsm;
  private MockedStatic<VFS> vfsMockedStatic;
  private MockedStatic<UriParser> uriParserMockedStatic;

  @Rule public final ExpectedException exception = ExpectedException.none();

  @Before
  public void setUp() {
    vfsMockedStatic = Mockito.mockStatic( VFS.class );
    uriParserMockedStatic = Mockito.mockStatic( UriParser.class );
    uriParserMockedStatic.when( () -> UriParser.encode( anyString(), any( char[].class ) ) ).thenCallRealMethod();
    uriParserMockedStatic.when( () -> UriParser.decode( anyString() ) ).thenCallRealMethod();
    uriParserMockedStatic.when( () -> UriParser.appendEncoded( any( StringBuilder.class ), anyString(), any( char[].class ) ) ).thenCallRealMethod();
    uriParserMockedStatic.when( () -> UriParser.canonicalizePath( any( StringBuilder.class ), anyInt(), anyInt(), any( FileNameParser.class ) ) ).thenCallRealMethod();
    uriParserMockedStatic.when( () -> UriParser.extractQueryString( any( StringBuilder.class ) ) ).thenCallRealMethod();
    uriParserMockedStatic.when( () -> UriParser.fixSeparators( any( StringBuilder.class ) ) ).thenCallRealMethod();
    uriParserMockedStatic.when( () -> UriParser.extractScheme( anyString(), any( StringBuilder.class ) ) ).thenCallRealMethod();

    fsm = mock( StandardFileSystemManager.class );
    vfsMockedStatic.when( VFS::getManager ).thenReturn( fsm );
  }

  @After
  public void cleanup() {
    vfsMockedStatic.close();
    uriParserMockedStatic.close();
    Mockito.validateMockitoUsage();
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
    return invocation -> {
      Object[] args = invocation.getArguments();
      ( (StringBuilder) args[2] ).append( buildPath );
      return prefix;
    };
  }

  private void buildExtractSchemeMocks( String prefix, String fullPath, String pathWithoutPrefix ) {
    String[] schemes = { "hdfs" };
    when( fsm.getSchemes() ).thenReturn( schemes );
    uriParserMockedStatic.when( () -> UriParser.extractScheme( eq( schemes ), eq( fullPath ), any( StringBuilder.class ) ) )
      .thenAnswer( buildSchemeAnswer( prefix, pathWithoutPrefix ) );
  }
}
