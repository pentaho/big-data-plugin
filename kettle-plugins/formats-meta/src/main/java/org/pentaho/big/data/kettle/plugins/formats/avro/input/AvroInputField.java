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

package org.pentaho.big.data.kettle.plugins.formats.avro.input;

import org.pentaho.big.data.kettle.plugins.formats.BaseFormatInputField;

import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.hadoop.shim.api.format.AvroSpec;
import org.pentaho.hadoop.shim.api.format.IAvroInputField;

import java.util.List;

/**
 * Base class for format's input/output field - path added.
 *
 * @author JRice <joseph.rice@hitachivantara.com>
 */
public class AvroInputField extends BaseFormatInputField implements IAvroInputField {

  ///////// Below added methods/variables to this object /////////////
  private List<String> pathParts;
  private List<String> indexedVals;

  private boolean m_isValid;
  protected String m_cleansedVariableName;
  protected String m_resolvedFieldName;
  //protected String m_resolvedDefaultValue;

  /** Index of this field in the incoming row stream */
  //private int m_inputIndex = -1;

  private String indexedValues;
  protected ValueMetaInterface m_fieldVM;
  /** The name of the variable to hold this field's values */
  public String m_variableName = "";

  private ValueMeta tempValueMeta;
  private List<String> tempParts;

  protected static Class<?> PKG = AvroInputMetaBase.class;

  public int getOutputIndex() {
    return outputIndex;
  }

  public void setOutputIndex( int outputIndex ) {
    this.outputIndex = outputIndex;
  }

  private int outputIndex; // the index that this field is in the output
  // row structure

  public void setPathParts( List<String> pathParts ) {
    this.pathParts = pathParts;
  }

  public void setIndexedVals( List<String> mindexedVals ) {
    this.indexedVals = mindexedVals;
  }

  public List<String> getPathParts() {

    return pathParts;
  }

  public List<String> getIndexedVals() {
    return indexedVals;
  }

  public ValueMeta getTempValueMeta() {
    return tempValueMeta;
  }

  public void setTempValueMeta( ValueMeta tempValueMeta ) {
    this.tempValueMeta = tempValueMeta;
  }

  public List<String> getTempParts() {
    return tempParts;
  }

  public void setTempParts( List<String> tempParts ) {
    this.tempParts = tempParts;
  }

  public String getIndexedValues() {
    return indexedValues;
  }

  public void setIndexedValues( String indexedValues ) {
    this.indexedValues = indexedValues;
  }

////////////////////  End added methods / variables ///////////////////////////////

  @Override
  public String getAvroFieldName() {
    return formatFieldName;
  }

  @Override public void setAvroFieldName( String avroFieldName ) {
    this.formatFieldName = avroFieldName;
  }

  @Override
  public AvroSpec.DataType getAvroType() {
    return AvroSpec.DataType.getDataType( getFormatType() );
  }

  @Override
  public void setAvroType( AvroSpec.DataType avroType ) {
    setFormatType( avroType.getId() );
  }

  @Injection( name = "AVRO_TYPE", group = "FIELDS" )
  @Override
  public void setAvroType( String avroType ) {
    for ( AvroSpec.DataType tmpType : AvroSpec.DataType.values() ) {
      if ( tmpType.getName().equalsIgnoreCase( avroType ) ) {
        setFormatType( tmpType.getId() );
        break;
      }
    }
  }

  @Override
  public String getDisplayableAvroFieldName() {
    String displayableAvroFieldName = formatFieldName;
    if ( formatFieldName.contains( FILENAME_DELIMITER ) ) {
      displayableAvroFieldName = formatFieldName.split( FILENAME_DELIMITER )[ 0 ];
    }

    return displayableAvroFieldName;
  }

  public String getTypeDesc() {
    return ValueMetaFactory.getValueMetaName( getPentahoType() );
  }


}
