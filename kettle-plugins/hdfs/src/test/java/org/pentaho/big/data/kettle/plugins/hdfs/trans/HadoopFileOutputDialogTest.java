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

package org.pentaho.big.data.kettle.plugins.hdfs.trans;

import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.apache.commons.vfs2.provider.UriParser;
import org.eclipse.swt.custom.CCombo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.Const;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.any;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import static org.mockito.ArgumentMatchers.eq;

/**
 * Created by bryan on 11/23/15.
 */
@RunWith( MockitoJUnitRunner.class )
public class HadoopFileOutputDialogTest {

  private static final String HDFS_PREFIX = "hdfs";
  private static final String MY_HOST_URL = "//myhost:8020";
  private StandardFileSystemManager fsm;
  private MockedStatic<UriParser> uriParserMockedStatic;
  private MockedStatic<VFS> vfsMockedStatic;

  @Before
  public void setUp() throws Exception {
    uriParserMockedStatic = Mockito.mockStatic( UriParser.class );
    vfsMockedStatic = Mockito.mockStatic( VFS.class );
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
  public void testGetUrlPathHdfsPrefix() {
    String prefix = HDFS_PREFIX;
    String pathBase = MY_HOST_URL;
    String expected = "/path/to/file";
    String fullPath = prefix + ":" + pathBase + expected;

    buildExtractSchemeMocks( prefix, fullPath, pathBase + expected );
    assertEquals( expected, HadoopFileOutputDialog.getUrlPath( fullPath ) );
  }

  @Test
  public void testGetUrlPathMapRPRefix() {
    String prefix = "maprfs";
    String pathBase = "//";
    String expected = "/path/to/file";
    String fullPath = prefix + ":" + pathBase + expected;

    buildExtractSchemeMocks( prefix, fullPath, pathBase + expected );
    assertEquals( expected, HadoopFileOutputDialog.getUrlPath( fullPath ) );
  }

  @Test
  public void testGetUrlPathSpecialPrefix() {
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
  public void testGetUrlPathRootPath() {
    String prefix = HDFS_PREFIX;
    String pathBase = MY_HOST_URL;
    String expected = "/";
    String fullPath = prefix + ":" + pathBase + expected;
    buildExtractSchemeMocks( prefix, fullPath, pathBase + expected );
    assertEquals( expected, HadoopFileOutputDialog.getUrlPath( fullPath ) );
  }

  @Test
  public void testGetUrlPathRootPathWithoutSlash() {
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

    return invocation -> {
      Object[] args = invocation.getArguments();
      ( (StringBuilder) args[2] ).append( buildPath );
      return prefix;
    };
  }

  private void buildExtractSchemeMocks( String prefix, String fullPath, String pathWithoutPrefix ) {
    uriParserMockedStatic.when( () -> UriParser.extractScheme( any( String[].class ), eq( fullPath ) ) ).thenReturn( prefix );
    uriParserMockedStatic.when( () -> UriParser.extractScheme( any( String[].class ), eq( fullPath ),
      any( StringBuilder.class ) ) ).thenAnswer( buildSchemeAnswer( prefix, pathWithoutPrefix ) );
  }
}
