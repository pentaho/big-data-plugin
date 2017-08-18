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

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceLocator;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.util.Utils;
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

  private EncodingType encodingType;
  private CompressionType compressionType;
  private ParquetVersion parquetVersion;

  // private final NamedClusterLoadSaveUtil namedClusterLoadSaveUtil;

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

  public String getEncoding() {
    return encodingType == null ? EncodingType.PLAIN.toString() : encodingType.toString();
  }

  public void setEncoding( String encoding ) {
    encodingType = parseFromToString( encoding, EncodingType.values(), null );
  }

  public String getCompression() {
    return compressionType == null ? CompressionType.NONE.toString() : compressionType.toString();
  }

  public void setCompression( String compression ) {
    compressionType = parseFromToString( compression, CompressionType.values(), null );
  }

  public String getParquetVersion() {
    return parquetVersion == null ? ParquetVersion.PARQUET_1.toString() : parquetVersion.toString();
  }

  public void setParquetVersion( String version ) {
    this.parquetVersion = parseFromToString( version, ParquetVersion.values(), null );
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

    private final String name;

    private CompressionType( String name ) {
      this.name = name;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  public static enum EncodingType {
    PLAIN( getMsg( "ParquetOutput.EncodingType.PLAIN" ) ), DICTIONARY( getMsg(
        "ParquetOutput.EncodingType.DICTIONARY" ) ), BIT_PACKED( getMsg(
            "ParquetOutput.EncodingType.BIT_PACKED" ) ), RLE( getMsg( "ParquetOutput.EncodingType.RLE" ) );

    private final String name;

    private EncodingType( String name ) {
      this.name = name;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  public static enum ParquetVersion {
    PARQUET_1( "Parquet 1.0" ), PARQUET_2( "Parquet 2.0" );

    private final String name;

    private ParquetVersion( String name ) {
      this.name = name;
    }

    @Override
    public String toString() {
      return name;
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
        if ( str.equalsIgnoreCase( type.toString() ) ) {
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
