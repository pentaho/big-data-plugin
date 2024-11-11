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

package org.pentaho.big.data.kettle.plugins.formats.orc.input;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.big.data.kettle.plugins.formats.FormatInputFile;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.named.cluster.NamedClusterEmbedManager;

public class OrcInputMetaBaseTest {

  private static final String FILE_NAME_VALID_PATH = "path/to/file";

  private OrcInputMetaBase inputMeta;
  private VariableSpace variableSpace;

  @Before
  public void setUp() throws Exception {
    NamedClusterEmbedManager  manager = mock( NamedClusterEmbedManager.class );

    TransMeta parentTransMeta = mock( TransMeta.class );
    doReturn( manager ).when( parentTransMeta ).getNamedClusterEmbedManager();

    StepMeta parentStepMeta = mock( StepMeta.class );
    doReturn( parentTransMeta ).when( parentStepMeta ).getParentTransMeta();

    inputMeta = new OrcInputMetaBase() {
      @Override
      public StepDataInterface getStepData() {
        return null;
      }     
      @Override
      public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
        return null;
      }
    };

    inputMeta.setParentStepMeta( parentStepMeta );
    inputMeta = spy( inputMeta );
    variableSpace = mock( VariableSpace.class );

    doReturn( "<def>" ).when( variableSpace ).environmentSubstitute( anyString() );
    doReturn( FILE_NAME_VALID_PATH ).when( variableSpace ).environmentSubstitute( FILE_NAME_VALID_PATH );
  }

  @Test
  public void testGetXmlWorksIfWeUpdateOnlyPartOfInputFilesInformation() throws Exception {
    inputMeta.inputFiles = new FormatInputFile();
    inputMeta.inputFiles.fileName = new String[] { FILE_NAME_VALID_PATH };

    inputMeta.getXML();

    assertEquals( inputMeta.inputFiles.fileName.length, inputMeta.inputFiles.fileMask.length );
    assertEquals( inputMeta.inputFiles.fileName.length, inputMeta.inputFiles.excludeFileMask.length );
    assertEquals( inputMeta.inputFiles.fileName.length, inputMeta.inputFiles.fileRequired.length );
    assertEquals( inputMeta.inputFiles.fileName.length, inputMeta.inputFiles.includeSubFolders.length );
    //specific for bigdata format
    assertEquals( inputMeta.inputFiles.fileName.length, inputMeta.inputFiles.environment.length );
  }

}
