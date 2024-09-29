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


package org.pentaho.di.ui.vfs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import org.apache.commons.vfs2.FileSystemOptions;
import org.eclipse.swt.widgets.Shell;
import org.junit.Test;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.vfs.ui.VfsFileChooserDialog;

public class VfsFileChooserHelperTest {

  private static final boolean TEST_SHOW_FILE_SCHEME_VALUE = false;
  private static final String S3_TEST_RESTRICTION = "s3";
  private static final String[] TEST_RESTRICTION_ARRAY = { "resttiction1", "resttiction2", null, "resttiction3" };
  private static final String DEFAULT_SCHEME_VALUE = "file";
  private static final String TEST_SCHEME_VALUE = "test scheme";

  private Shell shellMock = mock( Shell.class );
  private VfsFileChooserDialog fcDialogMock = mock( VfsFileChooserDialog.class );
  private VariableSpace varSpMock = mock( VariableSpace.class );
  private FileSystemOptions fsOptions = new FileSystemOptions();

  @Test
  public void testSetSchemeRestriction() {
    VfsFileChooserHelper helper = new VfsFileChooserHelper( shellMock, fcDialogMock, varSpMock, fsOptions );
    helper.setSchemeRestriction( S3_TEST_RESTRICTION );
    assertEquals( 1, helper.getSchemeRestrictions().length );
    assertEquals( S3_TEST_RESTRICTION, helper.getSchemeRestrictions()[0] );
  }

  @Test
  public void testGetSchemeRestrictionReturnsNull_ForDefaultEmptyRestriction() {
    VfsFileChooserHelper helper = new VfsFileChooserHelper( shellMock, fcDialogMock, varSpMock, fsOptions );
    assertNotNull( helper.getSchemeRestrictions() );
    assertEquals( 0, helper.getSchemeRestrictions().length );
    assertNull( helper.getSchemeRestriction() );
  }

  @Test
  public void testGetSchemeRestrictionReturnsRestriction_ForNotEmptyRestriction() {
    VfsFileChooserHelper helper = new VfsFileChooserHelper( shellMock, fcDialogMock, varSpMock, fsOptions );
    assertNotNull( helper.getSchemeRestrictions() );
    assertEquals( 0, helper.getSchemeRestrictions().length );
    helper.setSchemeRestriction( S3_TEST_RESTRICTION );
    assertEquals( S3_TEST_RESTRICTION, helper.getSchemeRestriction() );
  }

  @Test
  public void testConstructorWithParams() {
    VfsFileChooserHelper helper = new VfsFileChooserHelper( shellMock, fcDialogMock, varSpMock, fsOptions );
    assertNotNull( helper );
    assertSame( fcDialogMock, helper.getFileChooserDialog() );
    assertSame( shellMock, helper.getShell() );
    assertSame( varSpMock, helper.getVariableSpace() );
    assertSame( fsOptions, helper.getFileSystemOptions() );
    assertEquals( DEFAULT_SCHEME_VALUE, helper.getDefaultScheme() );
    assertTrue( helper.showFileScheme() );
    assertNotNull( helper.getSchemeRestrictions() );
    assertEquals( 0, helper.getSchemeRestrictions().length );
  }

  @Test
  public void testConstructorWithParamsWithoutFileSystemOptions() {
    VfsFileChooserHelper helper = new VfsFileChooserHelper( shellMock, fcDialogMock, varSpMock );
    assertNotNull( helper );
    assertSame( fcDialogMock, helper.getFileChooserDialog() );
    assertSame( shellMock, helper.getShell() );
    assertSame( varSpMock, helper.getVariableSpace() );
    assertNotNull( helper.getFileSystemOptions() );
    assertEquals( DEFAULT_SCHEME_VALUE, helper.getDefaultScheme() );
    assertTrue( helper.showFileScheme() );
    assertNotNull( helper.getSchemeRestrictions() );
    assertEquals( 0, helper.getSchemeRestrictions().length );
  }

  @Test
  public void testSetSchemeRestrictions_ForArrayOfRestrictionStrings() {
    VfsFileChooserHelper helper = new VfsFileChooserHelper( shellMock, fcDialogMock, varSpMock, fsOptions );
    helper.setSchemeRestrictions( TEST_RESTRICTION_ARRAY );
    assertEquals( TEST_RESTRICTION_ARRAY.length, helper.getSchemeRestrictions().length );
    assertArrayEquals( TEST_RESTRICTION_ARRAY, helper.getSchemeRestrictions() );
  }

  @Test
  public void testSetDefaultScheme() {
    VfsFileChooserHelper helper = new VfsFileChooserHelper( shellMock, fcDialogMock, varSpMock, fsOptions );
    assertEquals( DEFAULT_SCHEME_VALUE, helper.getDefaultScheme() );
    helper.setDefaultScheme( TEST_SCHEME_VALUE );
    assertEquals( TEST_SCHEME_VALUE, helper.getDefaultScheme() );
  }

  @Test
  public void testSetShowFileScheme() {
    VfsFileChooserHelper helper = new VfsFileChooserHelper( shellMock, fcDialogMock, varSpMock, fsOptions );
    assertTrue( helper.showFileScheme() );
    helper.setShowFileScheme( TEST_SHOW_FILE_SCHEME_VALUE );
    assertEquals( TEST_SHOW_FILE_SCHEME_VALUE, helper.showFileScheme() );
  }

  @Test
  public void testSetVariableSpace() {
    VfsFileChooserHelper helper = new VfsFileChooserHelper( shellMock, fcDialogMock, varSpMock, fsOptions );
    VariableSpace oneMoreVarSpMock = mock( VariableSpace.class );
    assertSame( varSpMock, helper.getVariableSpace() );
    helper.setVariableSpace( oneMoreVarSpMock );
    assertSame( oneMoreVarSpMock, helper.getVariableSpace() );
  }

  @Test
  public void testSetFileSystemOptions() {
    VfsFileChooserHelper helper = new VfsFileChooserHelper( shellMock, fcDialogMock, varSpMock, fsOptions );
    FileSystemOptions oneMoreFsOptions = new FileSystemOptions();
    assertSame( fsOptions, helper.getFileSystemOptions() );
    helper.setFileSystemOptions( oneMoreFsOptions );
    assertSame( oneMoreFsOptions, helper.getFileSystemOptions() );
  }

  @Test
  public void testReturnsUserAuthenticatedFileObjects() {
    VfsFileChooserHelper helper = new VfsFileChooserHelper( shellMock, fcDialogMock, varSpMock, fsOptions );
    assertFalse( helper.returnsUserAuthenticatedFileObjects() );
  }

}
