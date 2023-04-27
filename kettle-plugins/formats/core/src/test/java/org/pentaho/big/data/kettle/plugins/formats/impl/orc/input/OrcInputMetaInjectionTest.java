/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2020 by Hitachi Vantara : http://www.pentaho.com
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
