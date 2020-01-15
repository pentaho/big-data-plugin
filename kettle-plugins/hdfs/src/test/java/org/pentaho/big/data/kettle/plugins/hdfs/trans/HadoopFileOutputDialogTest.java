/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.big.data.kettle.plugins.hdfs.trans;

import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.apache.commons.vfs2.provider.UriParser;
import org.eclipse.swt.custom.CCombo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.Const;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.any;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import static org.mockito.Matchers.eq;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Created by bryan on 11/23/15.
 */
@RunWith( PowerMockRunner.class )
@PrepareForTest( { VFS.class, UriParser.class } )
public class HadoopFileOutputDialogTest {

  private static final String[] SCHEMES = { "hdfs", "maprfs", "mySpecialPrefix" };
  private static final String HDFS_PREFIX = "hdfs";
  private static final String MY_HOST_URL = "//myhost:8020";
  private StandardFileSystemManager fsm;

  @Before
  public void setUp() throws Exception {
    mockStatic( UriParser.class );
    mockStatic( VFS.class );
    fsm = mock( StandardFileSystemManager.class );
    when( VFS.getManager() ).thenReturn( fsm );
    when( fsm.getSchemes() ).thenReturn( SCHEMES );
  }

  @Test
  public void testGetUrlPathHdfsPrefix() throws Exception {
    String prefix = HDFS_PREFIX;
    String pathBase = MY_HOST_URL;
    String expected = "/path/to/file";
    String fullPath = prefix + ":" + pathBase + expected;

    buildExtractSchemeMocks( prefix, fullPath, pathBase + expected );
    assertEquals( expected, HadoopFileOutputDialog.getUrlPath( fullPath ) );
  }

  @Test
  public void testGetUrlPathMapRPRefix() throws Exception  {
    String prefix = "maprfs";
    String pathBase = "//";
    String expected = "/path/to/file";
    String fullPath = prefix + ":" + pathBase + expected;

    buildExtractSchemeMocks( prefix, fullPath, pathBase + expected );
    assertEquals( expected, HadoopFileOutputDialog.getUrlPath( fullPath ) );
  }

  @Test
  public void testGetUrlPathSpecialPrefix() throws Exception {
    String prefix = "mySpecialPrefix";
    String pathBase = "//host";
    String expected = "/path/to/file";
    String fullPath = prefix + ":" + pathBase + expected;

    buildExtractSchemeMocks( prefix, fullPath, pathBase + expected );
    assertEquals( expected, HadoopFileOutputDialog.getUrlPath( fullPath ) );
  }

  @Test
  public void testGetUrlPathNoPrefix() {
    String expected = "/path/to/file";
    assertEquals( expected, HadoopFileOutputDialog.getUrlPath( expected ) );
  }

  @Test
  public void testGetUrlPathVariablePrefix() {
    String expected = "${myTestVar}";
    assertEquals( expected, HadoopFileOutputDialog.getUrlPath( expected ) );
  }

  @Test
  public void testGetUrlPathRootPath() throws Exception  {
    String prefix = HDFS_PREFIX;
    String pathBase = MY_HOST_URL;
    String expected = "/";
    String fullPath = prefix + ":" + pathBase + expected;
    buildExtractSchemeMocks( prefix, fullPath, pathBase + expected );
    assertEquals( expected, HadoopFileOutputDialog.getUrlPath( fullPath ) );
  }

  @Test
  public void testGetUrlPathRootPathWithoutSlash() throws Exception  {
    String prefix = HDFS_PREFIX;
    String pathBase = MY_HOST_URL;
    String expected = "/";
    String fullPath = prefix + ":" + pathBase;
    buildExtractSchemeMocks( prefix, fullPath, pathBase );
    assertEquals( expected, HadoopFileOutputDialog.getUrlPath( fullPath ) );
  }

  @Test
  public void testFillWithSupportedDateFormats() {
    HadoopFileOutputDialog dialog = mock( HadoopFileOutputDialog.class );
    CCombo combo = mock( CCombo.class );

    String[] dates = Const.getDateFormats();
    assertEquals( 20, dates.length );

    // currently there are 20 date formats, 10 of which contain ':' characters which are illegal in hadoop filenames
    // if the formats returned change, the numbers on this test should be adjusted

    doCallRealMethod().when( dialog ).fillWithSupportedDateFormats( any(), any() );
    dialog.fillWithSupportedDateFormats( combo, dates );

    verify( combo, times( 10 ) ).add( any() );
  }

  private Answer buildSchemeAnswer( String prefix, String buildPath ) {
    Answer extractSchemeAnswer = invocation -> {
      Object[] args = invocation.getArguments();
      ( (StringBuilder) args[2] ).append( buildPath );
      return prefix;
    };

    return extractSchemeAnswer;
  }

  private void buildExtractSchemeMocks( String prefix, String fullPath, String pathWithoutPrefix ) {
    when( UriParser.extractScheme( any( String[].class ), eq( fullPath ) ) ).thenReturn( prefix );
    when( UriParser.extractScheme( any( String[].class ), eq( fullPath ),
      any( StringBuilder.class ) ) ).thenAnswer( buildSchemeAnswer( prefix, pathWithoutPrefix ) );
  }
}
