/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2021 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.big.data.kettle.plugins.formats.orc.output;

import org.junit.Test;
import org.pentaho.hadoop.shim.api.format.OrcSpec;

import static org.junit.Assert.*;

public class OrcOutputFieldTest {

  @Test
  public void setFormatTypeTest() {
    //Names must be unique to each data type and should be addressable like the id
    OrcOutputField f;
    for ( OrcSpec.DataType dataType : OrcSpec.DataType.values() ) {

      //Set by Name
      f = new OrcOutputField();
      f.setFormatType( dataType.getName() );
      assertEquals( "Checking setting of \"" + dataType.getName() + "\"", dataType, f.getOrcType() );

      //Set by Id
      f = new OrcOutputField();
      f.setFormatType( String.valueOf( dataType.getId() ) );
      assertEquals( "Checking setting of \"" + dataType.getId() + "\"", dataType, f.getOrcType() );

      //Set by Enum
      f = new OrcOutputField();
      f.setFormatType( String.valueOf( dataType.toString() ) );
      assertEquals( "Checking setting of \"" + dataType.toString() + "\"", dataType, f.getOrcType() );
    }
  }
}