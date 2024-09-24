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

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.apache.commons.vfs2.provider.FileNameParser;
import org.apache.commons.vfs2.provider.UriParser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class MapRFileNameParserTest {
  private static final String PREFIX = "maprfs";
  private static final String BASE_PATH = "//";
  private static final String BASE_URI = PREFIX + ":" + BASE_PATH;

  private StandardFileSystemManager fsm;
  private MockedStatic<VFS> vfsMockedStatic;
  private MockedStatic<UriParser> uriParserMockedStatic;

  @Before
  public void setUp() {
    vfsMockedStatic = Mockito.mockStatic( VFS.class );
    uriParserMockedStatic = Mockito.mockStatic( UriParser.class );
    uriParserMockedStatic.when( () -> UriParser.encode( anyString(), any( char[].class ) ) ).thenCallRealMethod();
    uriParserMockedStatic.when( () -> UriParser.decode( anyString() ) ).thenCallRealMethod();
    uriParserMockedStatic.when( () -> UriParser.appendEncoded( any( StringBuilder.class ), anyString(), any( char[].class ) ) ).thenCallRealMethod();

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
    assertEquals( -1, MapRFileNameParser.getInstance().getDefaultPort() );
  }

  @Test
  public void rootPathNoClusterName() throws Exception {
    final String FILEPATH = "/";
    final String URI = BASE_URI + FILEPATH;

    buildExtractSchemeMocks( PREFIX, URI, BASE_PATH + FILEPATH );

    FileNameParser parser = MapRFileNameParser.getInstance();
    FileName name = parser.parseUri( null, null, URI );

    assertEquals( URI, name.getURI() );
    assertEquals( PREFIX, name.getScheme() );
  }

  @Test
  public void withPath() throws Exception {
    final String FILEPATH = "/my/file/path";
    final String URI = BASE_URI + FILEPATH;

    buildExtractSchemeMocks( PREFIX, URI, BASE_PATH + FILEPATH );

    FileNameParser parser = MapRFileNameParser.getInstance();
    FileName name = parser.parseUri( null, null, URI );

    assertEquals( URI, name.getURI() );
    assertEquals( PREFIX, name.getScheme() );
    assertEquals( FILEPATH, name.getPath() );
  }

  @Test
  public void withPathAndClusterName() throws Exception {
    final String HOST = "cluster2";
    final String FILEPATH = "/my/file/path";
    final String URI = BASE_URI + HOST + FILEPATH;

    buildExtractSchemeMocks( PREFIX, URI, BASE_PATH + HOST + FILEPATH );

    FileNameParser parser = MapRFileNameParser.getInstance();
    FileName name = parser.parseUri( null, null, URI );

    assertEquals( URI, name.getURI() );
    assertEquals( PREFIX, name.getScheme() );
    assertTrue( name.getURI().startsWith( PREFIX + ":" + BASE_PATH + HOST ) );
    assertEquals( FILEPATH, name.getPath() );
  }

  private Answer buildSchemeAnswer( String prefix, String buildPath ) {
    return invocation -> {
      Object[] args = invocation.getArguments();
      ( (StringBuilder) args[2] ).append( buildPath );
      return prefix;
    };
  }

  private void buildExtractSchemeMocks( String prefix, String fullPath, String pathWithoutPrefix ) {
    String[] schemes = { "maprfs" };
    when( fsm.getSchemes() ).thenReturn( schemes );
    uriParserMockedStatic.when( () -> UriParser.extractScheme( eq( schemes ), eq( fullPath ), any( StringBuilder.class ) ) )
      .thenAnswer( buildSchemeAnswer( prefix, pathWithoutPrefix ) );
  }
}
