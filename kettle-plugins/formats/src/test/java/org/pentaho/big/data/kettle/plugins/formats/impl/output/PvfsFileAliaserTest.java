/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package org.pentaho.big.data.kettle.plugins.formats.impl.output;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.hadoop.shim.api.format.IPvfsAliasGenerator;

import java.io.File;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class PvfsFileAliaserTest {
  PvfsFileAliaser pvfsFileAliaser;
  @Mock
  VariableSpace variableSpace;
  @Mock
  IPvfsAliasGenerator aliasGenerator;
  @Mock
  LogChannelInterface log;

  private static final String TEMP_DIR_PREFIX = "PvfsFileAliaserTest";
  private static String finalPath;
  private static File finalFile;
  private String temporaryPath;

  @BeforeClass
  public static void setup() throws Exception {
    finalPath = Files.createTempDirectory( TEMP_DIR_PREFIX ) + File.separator + "finalFile";
    finalFile = new File( finalPath );
  }

  @Before
  public void setUp() throws Exception {
    finalFile.delete();
    temporaryPath = Files.createTempDirectory( TEMP_DIR_PREFIX ) + File.separator + "temporaryile";
    new File( temporaryPath )
      .createNewFile();  //create the alias file so it and it's parent can be successfully deleted
    when( aliasGenerator.generateAlias( anyString() ) ).thenReturn( temporaryPath );
    pvfsFileAliaser = new PvfsFileAliaser( finalPath, variableSpace, aliasGenerator, true, log );
  }

  @Test
  public void testGenerateWithActiveAlias() throws Exception {
    String aliasPath = pvfsFileAliaser.generateAlias();
    assertEquals( temporaryPath, aliasPath );
    assertFalse( finalFile.exists() );
    pvfsFileAliaser.copyFileToFinalDestination();
    assertTrue( finalFile.exists() );
    pvfsFileAliaser.deleteTempFileAndFolder();
    assertFalse( new File( new File( temporaryPath ).getParent() ).exists() );
  }

  @Test
  public void testGenerateWithInactiveAlias() throws Exception {
    when( aliasGenerator.generateAlias( anyString() ) ).thenReturn( null );
    String aliasPath = pvfsFileAliaser.generateAlias();
    assertEquals( finalPath, aliasPath );
    assertFalse( finalFile.exists() );
    pvfsFileAliaser.copyFileToFinalDestination();
    assertFalse( finalFile.exists() );
  }

  @Test
  public void testCopyFileToFinalDestinationWithoutGenerate() throws Exception {
    pvfsFileAliaser.copyFileToFinalDestination();
    assertFalse( finalFile.exists() );
    assertTempFileExistsAndDelete();
  }

  @Test
  public void testDeleteTempFileAndFolderWithoutGenerate() {
    pvfsFileAliaser.deleteTempFileAndFolder();
    assertFalse( finalFile.exists() );
    assertTempFileExistsAndDelete();
  }

  private void assertTempFileExistsAndDelete() {
    File tempFile = new File( temporaryPath );
    assertTrue( tempFile.exists() );
    deleteTempFile();
  }

  private void deleteTempFile() {
    File tempFile = new File( temporaryPath );
    tempFile.delete();
    new File( tempFile.getParent() ).delete();
  }
}