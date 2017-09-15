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

package org.pentaho.big.data.kettle.plugins.formats.impl.parquet.output;

import java.util.function.Function;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceLocator;
import org.pentaho.big.data.kettle.plugins.formats.parquet.output.ParquetOutputMetaBase;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
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

  private static final Class<?> PKG = ParquetOutputMeta.class;

  private final NamedClusterServiceLocator namedClusterServiceLocator;
  private final NamedClusterService namedClusterService;

  public ParquetOutputMeta( NamedClusterServiceLocator namedClusterServiceLocator,
      NamedClusterService namedClusterService ) {
    this.namedClusterServiceLocator = namedClusterServiceLocator;
    this.namedClusterService = namedClusterService;
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {
    return new ParquetOutput( stepMeta, stepDataInterface, copyNr, transMeta, trans, namedClusterServiceLocator );
  }

  public NamedCluster getNamedCluster() {
    return namedClusterService.getClusterTemplate();
  }

  @Override
  public StepDataInterface getStepData() {
    return new ParquetOutputData();
  }

  public EncodingType getEncodingType( VariableSpace vspace ) {
    return enableDictionary ? EncodingType.DICTIONARY : EncodingType.PLAIN;
  }

  public void setEncodingType( String encoding ) {
    enableDictionary = encoding != null ? encoding.startsWith( "D" ) : false;
  }

  public String getEncodingType() {
    return enableDictionary ? EncodingType.DICTIONARY.uiName : EncodingType.PLAIN.uiName;
  }

  public int getRowGroupSize( VariableSpace vspace ) {
    return parseReplace( rowGroupSize, vspace, str -> Integer.parseInt( str ), 0 );
  }

  public String getRowGroupSize() {
    return rowGroupSize;
  }

  public void setRowGroupSize( String value ) {
    rowGroupSize = value;
  }

  public String getCompressionType() {
    return StringUtil.isVariable( compressionType ) ? compressionType : getCompressionType( null ).toString();
  }

  public void setCompressionType( String value ) {
    compressionType =
        StringUtil.isVariable( value ) ? value : parseFromToString( value, CompressionType.values(), null ).name();
  }

  public CompressionType getCompressionType( VariableSpace vspace ) {
    return parseReplace( compressionType, vspace, str -> CompressionType.valueOf( str ), CompressionType.NONE );
  }

  public String getParquetVersion() {
    return StringUtil.isVariable( parquetVersion ) ? parquetVersion : getParquetVersion( null ).toString();
  }

  public void setParquetVersion( String value ) {
    parquetVersion =
        StringUtil.isVariable( value ) ? value : parseFromToString( value, ParquetVersion.values(), null ).name();
  }

  public ParquetVersion getParquetVersion( VariableSpace vspace ) {
    return parseReplace( parquetVersion, vspace, str -> ParquetVersion.valueOf( str ), ParquetVersion.PARQUET_1 );
  }

  public int getDataPageSize( VariableSpace vspace ) {
    return parseReplace( dataPageSize, vspace, s -> Integer.parseInt( s ), 0 );
  }

  public String getDataPageSize() {
    return dataPageSize;
  }

  public void setDataPageSize( String dataPageSize ) {
    this.dataPageSize = dataPageSize;
  }

  public int getDictPageSize( VariableSpace vspace ) {
    if ( getEncodingType( vspace ).equals( EncodingType.DICTIONARY ) ) {
      return parseReplace( dictPageSize, vspace, s -> Integer.parseInt( s ), 0 );
    } else {
      return 0;
    }
  }

  public String getDictPageSize() {
    return dictPageSize;
  }

  public void setDictPageSize( String dictPageSize ) {
    this.dictPageSize = dictPageSize;
  }

  private <T> T parseReplace( String value, VariableSpace vspace, Function<String, T> parser, T defaultValue ) {
    String replaced = vspace != null ? vspace.environmentSubstitute( value ) : value;
    if ( !Utils.isEmpty( replaced ) ) {
      try {
        return parser.apply( replaced );
      } catch ( Exception e ) {
        // ignored
      }
    }
    return defaultValue;
  }

  public String[] getEncodingTypes() {
    return getStrings( EncodingType.values() );
  }

  public String[] getCompressionTypes() {
    return getStrings( CompressionType.values() );
  }

  public String[] getVersionTypes() {
    return getStrings( ParquetVersion.values() );
  }

  public static enum CompressionType {
    NONE( getMsg( "ParquetOutput.CompressionType.NONE" ) ), SNAPPY( "Snappy" ), GZIP( "GZIP" ), LZO( "LZO" );

    private final String uiName;

    private CompressionType( String name ) {
      this.uiName = name;
    }

    @Override
    public String toString() {
      return uiName;
    }
  }

  public static enum EncodingType {
    PLAIN( getMsg( "ParquetOutput.EncodingType.PLAIN" ) ), DICTIONARY( getMsg(
        "ParquetOutput.EncodingType.DICTIONARY" ) ), BIT_PACKED( getMsg(
            "ParquetOutput.EncodingType.BIT_PACKED" ) ), RLE( getMsg( "ParquetOutput.EncodingType.RLE" ) );

    private final String uiName;

    private EncodingType( String name ) {
      this.uiName = name;
    }

    @Override
    public String toString() {
      return uiName;
    }
  }

  public static enum ParquetVersion {
    PARQUET_1( "Parquet 1.0" ), PARQUET_2( "Parquet 2.0" );

    private final String uiName;

    private ParquetVersion( String name ) {
      this.uiName = name;
    }

    @Override
    public String toString() {
      return uiName;
    }
  }

  protected static <T> String[] getStrings( T[] objects ) {
    String[] names = new String[objects.length];
    int i = 0;
    for ( T obj : objects ) {
      names[i++] = obj.toString();
    }
    return names;
  }

  protected static <T> T parseFromToString( String str, T[] values, T defaultValue ) {
    if ( !Utils.isEmpty( str ) ) {
      for ( T type : values ) {
        if ( str.equals( type.toString() ) ) {
          return type;
        }
      }
    }
    return defaultValue;
  }

  private static String getMsg( String key ) {
    return BaseMessages.getString( PKG, key );
  }
}
