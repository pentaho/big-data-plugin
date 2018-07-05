/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
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
      if ( tmpType.getName().equalsIgnoreCase( orcType ) ) {
        setFormatType( tmpType.getId() );
        break;
      }
    }
  }

  public String getTypeDesc() {
    return ValueMetaFactory.getValueMetaName( getPentahoType() );
  }

}
