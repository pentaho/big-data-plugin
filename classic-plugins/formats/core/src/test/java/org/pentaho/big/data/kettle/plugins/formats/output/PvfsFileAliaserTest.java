/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2020-2022 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.big.data.kettle.plugins.formats.output;

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