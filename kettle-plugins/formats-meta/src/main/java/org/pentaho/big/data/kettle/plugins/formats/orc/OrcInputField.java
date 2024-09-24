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

package org.pentaho.big.data.kettle.plugins.formats.orc;

import org.pentaho.big.data.kettle.plugins.formats.BaseFormatInputField;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.hadoop.shim.api.format.IOrcInputField;
import org.pentaho.hadoop.shim.api.format.OrcSpec;

/**
 * @Author tkafalas
 */
public class OrcInputField extends BaseFormatInputField implements IOrcInputField {
  public OrcSpec.DataType getOrcType() {
    return OrcSpec.DataType.getDataType( getFormatType() );
  }

  @Override
  public void setOrcType( OrcSpec.DataType orcType ) {
    setFormatType( orcType.getId() );
  }

  @Injection( name = "ORC_TYPE", group = "FIELDS" )
  @Override
  public void setOrcType( String orcType ) {
    for ( OrcSpec.DataType tmpType : OrcSpec.DataType.values() ) {
      if ( tmpType.toString().equalsIgnoreCase( orcType ) ) {
        setFormatType( tmpType.getId() );
        break;
      }
    }
  }

  @Override
  public String getTypeDesc() {
    return ValueMetaFactory.getValueMetaName( getPentahoType() );
  }

}
