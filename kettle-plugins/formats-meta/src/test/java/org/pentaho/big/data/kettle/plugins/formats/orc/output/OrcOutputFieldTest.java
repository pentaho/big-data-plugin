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