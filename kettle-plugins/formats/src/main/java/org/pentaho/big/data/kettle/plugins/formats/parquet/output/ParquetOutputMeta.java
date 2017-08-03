/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2017 by Pentaho : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.formats.parquet.output;

import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;


@Step( id = "ParquetOutput", image = "PO.svg", name = "ParquetOutput.Name", description = "ParquetOutput.Description",
    categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.BigData",
    documentationUrl = "http://wiki.pentaho.com/display/EAI/Parquet+output",
    i18nPackageName = "org.pentaho.di.trans.steps.parquet" )
public class ParquetOutputMeta extends ParquetOutputMetaBase {

  private EncodingType encodingType;

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {
    return new ParquetOutput( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }


  @Override
  public StepDataInterface getStepData() {
    return new ParquetOutputData();
  }

  public String getEncoding() {
    return encodingType == null ? null : encodingType.toString();
  }

  public void setEncoding( String encoding ) {
    encodingType = EncodingType.parse( encoding );
  }

  public String[] getEncodingTypes() {
    String[] types = new String[EncodingType.values().length];
    int i = 0;
    for ( EncodingType et : EncodingType.values() ) {
      types[i++] = et.toString();
    }
    return types;
  }

  // TODO cf
  public static enum EncodingType {
    PLAIN( "Plain" ),
    DICTIONARY( "Dictionary" ),
    BIT_PACKED( "Bit packed" ),
    RLE( "RLE" );

    private final String name;
    private EncodingType( String name ) {
      this.name = name;
    }

    public static EncodingType parse( String str ) {
      if ( !Utils.isEmpty( str ) ) {
        for ( EncodingType type : EncodingType.values() ) {
          if ( str.equalsIgnoreCase( type.name ) ) {
            return type;
          }
        }
      }
      return null;
    }

    @Override
    public String toString() {
      return name;
    }
  }
}
