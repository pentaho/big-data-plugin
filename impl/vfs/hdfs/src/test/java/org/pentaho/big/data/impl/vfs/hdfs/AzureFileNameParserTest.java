/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2015-2021 by Hitachi Vantara : http://www.pentaho.com
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

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.apache.commons.vfs2.provider.FileNameParser;
import org.apache.commons.vfs2.provider.UriParser;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.doAnswer;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith( PowerMockRunner.class )
@PrepareForTest( { UriParser.class, VFS.class } )
public class AzureFileNameParserTest {

  private static final String BASE_PATH = "//";
  private static final String WASB_PREFIX = "wasb";
  private static final String WASB_BASE_URI = WASB_PREFIX + ":" + BASE_PATH;
  private static final String ABFS_PREFIX = "abfs";
  private static final String ABFS_BASE_URI = ABFS_PREFIX + ":" + BASE_PATH;

  private StandardFileSystemManager fsm;

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
    assertEquals( -1, AzureHdInsightsFileNameParser.getInstance().getDefaultPort() );
  }

  @Test
  public void rootPathNoClusterNameWasb() throws Exception {
    final String FILEPATH = "/";
    final String URI = WASB_BASE_URI + FILEPATH;

    buildExtractSchemeMocks( WASB_PREFIX, URI, BASE_PATH + FILEPATH );

    FileNameParser parser = AzureHdInsightsFileNameParser.getInstance();
    FileName name = parser.parseUri( null, null, URI );

    assertEquals( URI, name.getURI() );
    assertEquals( WASB_PREFIX, name.getScheme() );
  }

  @Test
  public void withPathWasb() throws Exception {
    final String FILEPATH = "/my/file/path";
    final String URI = WASB_BASE_URI + FILEPATH;

    buildExtractSchemeMocks( WASB_PREFIX, URI, BASE_PATH + FILEPATH );

    FileNameParser parser = AzureHdInsightsFileNameParser.getInstance();
    FileName name = parser.parseUri( null, null, URI );

    assertEquals( URI, name.getURI() );
    assertEquals( WASB_PREFIX, name.getScheme() );
    assertEquals( FILEPATH, name.getPath() );
  }

  @Test
  public void withPathAndClusterNameWasb() throws Exception {
    final String HOST = "cluster2";
    final String FILEPATH = "/my/file/path";
    final String URI = WASB_BASE_URI + HOST + FILEPATH;

    buildExtractSchemeMocks( WASB_PREFIX, URI, BASE_PATH + HOST + FILEPATH );

    FileNameParser parser = AzureHdInsightsFileNameParser.getInstance();
    FileName name = parser.parseUri( null, null, URI );

    assertEquals( URI, name.getURI() );
    assertEquals( WASB_PREFIX, name.getScheme() );
    assertTrue( name.getURI().startsWith( WASB_PREFIX + ":" + BASE_PATH + HOST ) );
    assertEquals( FILEPATH, name.getPath() );
  }

  @Test
  public void rootPathNoClusterNameAbfs() throws Exception {
    final String FILEPATH = "/";
    final String URI = ABFS_BASE_URI + FILEPATH;

    buildExtractSchemeMocks( ABFS_PREFIX, URI, BASE_PATH + FILEPATH );

    FileNameParser parser = AzureHdInsightsFileNameParser.getInstance();
    FileName name = parser.parseUri( null, null, URI );

    assertEquals( URI, name.getURI() );
    assertEquals( ABFS_PREFIX, name.getScheme() );
  }

  @Test
  public void withPathAbfs() throws Exception {
    final String FILEPATH = "/my/file/path";
    final String URI = ABFS_BASE_URI + FILEPATH;

    buildExtractSchemeMocks( ABFS_PREFIX, URI, BASE_PATH + FILEPATH );

    FileNameParser parser = AzureHdInsightsFileNameParser.getInstance();
    FileName name = parser.parseUri( null, null, URI );

    assertEquals( URI, name.getURI() );
    assertEquals( ABFS_PREFIX, name.getScheme() );
    assertEquals( FILEPATH, name.getPath() );
  }

  @Test
  public void withPathAndClusterNameAbfs() throws Exception {
    final String HOST = "cluster2";
    final String FILEPATH = "/my/file/path";
    final String URI = ABFS_BASE_URI + HOST + FILEPATH;

    buildExtractSchemeMocks( ABFS_PREFIX, URI, BASE_PATH + HOST + FILEPATH );

    FileNameParser parser = AzureHdInsightsFileNameParser.getInstance();
    FileName name = parser.parseUri( null, null, URI );

    assertEquals( URI, name.getURI() );
    assertEquals( ABFS_PREFIX, name.getScheme() );
    assertTrue( name.getURI().startsWith( ABFS_PREFIX + ":" + BASE_PATH + HOST ) );
    assertEquals( FILEPATH, name.getPath() );
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
    String[] schemes = { "wasb", "abfs" };
    when( fsm.getSchemes() ).thenReturn( schemes );
    doAnswer( buildSchemeAnswer( prefix, pathWithoutPrefix ) ).when( UriParser.class, "extractScheme",
      eq( schemes ), eq( fullPath ), any( StringBuilder.class ) );
  }
}
