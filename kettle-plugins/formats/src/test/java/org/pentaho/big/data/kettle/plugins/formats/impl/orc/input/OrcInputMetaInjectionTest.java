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


package org.pentaho.big.data.kettle.plugins.formats.impl.orc.input;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.pentaho.big.data.kettle.plugins.formats.impl.NamedClusterResolver;
import org.pentaho.big.data.kettle.plugins.formats.orc.OrcInputField;
import org.pentaho.di.core.injection.BaseMetadataInjectionTest;
import org.pentaho.di.junit.rules.RestorePDIEngineEnvironment;
import org.pentaho.hadoop.shim.api.format.OrcSpec;

import static org.mockito.Mockito.mock;

public class OrcInputMetaInjectionTest extends BaseMetadataInjectionTest<OrcInputMeta> {
  @ClassRule public static RestorePDIEngineEnvironment env = new RestorePDIEngineEnvironment();

  @Before
  public void setup() {
    NamedClusterResolver mockNamedClusterResolver = mock( NamedClusterResolver.class );
    setup( new OrcInputMeta( mockNamedClusterResolver ) );
    OrcInputField orcInputField = new OrcInputField();
    meta.setInputFields( new OrcInputField[] { orcInputField } );
  }

  @Test
  public void test() throws Exception {

    check( "FILENAME", () -> meta.inputFiles.fileName[0] );
    checkStringToEnum( "ORC_TYPE", () -> meta.getInputFields()[0].getOrcType(), OrcSpec.DataType.class );

    check( "FIELD_PATH", () -> meta.getInputFields()[ 0 ].getFormatFieldName() );
    check( "FIELD_NAME", () -> meta.getInputFields()[ 0 ].getName() );
    checkPdiTypes( "FIELD_TYPE", () -> meta.getInputFields()[ 0 ].getType() );
  }

}
