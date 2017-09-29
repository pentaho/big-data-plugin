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

import org.apache.commons.vfs2.FileObject;
import org.pentaho.big.data.kettle.plugins.formats.FormatInputOutputField;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.vfs.AliasedFileObject;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.workarounds.ResolvableResource;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.function.Function;

/**
 * Parquet output meta step without Hadoop-dependent classes. Required for read meta in the spark native code.
 *
 * @author <alexander_buloichik@epam.com>
 */
public abstract class ParquetOutputMetaBase extends BaseStepMeta implements StepMetaInterface, ResolvableResource {

  private static final Class<?> PKG = ParquetOutputMetaBase.class;

  @Injection( name = "COMPRESSION" )
  public String compressionType;
  @Injection( name = "PARQUET_VERSION" )
  public String parquetVersion;
  @Injection( name = "ROW_GROUP_SIZE" )
  public String rowGroupSize;
  @Injection( name = "DATA_PAGE_SIZE" )
  public String dataPageSize;
  @Injection( name = "ENABLE_DICTIONARY" )
  public boolean enableDictionary;
  @Injection( name = "DICT_PAGE_SIZE" )
  public String dictPageSize;
  @Injection( name = "OVERRIDE_OUTPUT" )
  public boolean overrideOutput;

  /** Flag: add the date in the filename */
  @Injection( name = "INC_DATE_IN_FILENAME" )
  private boolean dateInFilename;

  /** Flag: add the time in the filename */
  @Injection( name = "INC_TIME_IN_FILENAME" )
  private boolean timeInFilename;

  @Injection( name = "DATE_FORMAT" )
  private String dateTimeFormat;

  /** The file extention in case of a generated filename */
  @Injection( name = "EXTENSION" )
  private String extension;

  public String filename;

  public FormatInputOutputField[] outputFields = new FormatInputOutputField[0];

  @Override
  public void setDefault() {
    outputFields = new FormatInputOutputField[ 0 ];
  }

  public String getFilename() {
    return filename;
  }

  public void setFilename( String filename ) {
    this.filename = filename;
  }

  @Override
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    readData( stepnode, metaStore );
  }

  private void readData( Node stepnode, IMetaStore metastore ) throws KettleXMLException {
    try {
      filename = XMLHandler.getTagValue( stepnode, "filename" );
      enableDictionary = Boolean.parseBoolean( XMLHandler.getTagValue( stepnode, "enableDictionary" ) );
      compressionType = XMLHandler.getTagValue( stepnode, "compression" );
      parquetVersion = XMLHandler.getTagValue( stepnode, "parquetVersion" );
      rowGroupSize = XMLHandler.getTagValue( stepnode, "rowGroupSize" );
      dataPageSize = XMLHandler.getTagValue( stepnode, "dataPageSize" );
      dictPageSize = XMLHandler.getTagValue( stepnode, "dictPageSize" );
      Node fields = XMLHandler.getSubNode( stepnode, "fields" );
      int nrfields = XMLHandler.countNodes( fields, "field" );
      List<FormatInputOutputField> parquetOutputFields = new ArrayList<>();
      for ( int i = 0; i < nrfields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );
        FormatInputOutputField outputField = new FormatInputOutputField();
        outputField.setPath( XMLHandler.getTagValue( fnode, "path" ) );
        outputField.setName( XMLHandler.getTagValue( fnode, "name" ) );
        outputField.setType( XMLHandler.getTagValue( fnode, "type" ) );
        outputField.setNullString( XMLHandler.getTagValue( fnode, "nullable" ) );
        outputField.setIfNullValue( XMLHandler.getTagValue( fnode, "default" ) );
        parquetOutputFields.add( outputField );
      }
      this.outputFields = parquetOutputFields.toArray( new FormatInputOutputField[ 0 ] );
    } catch ( Exception e ) {
      throw new KettleXMLException( "Unable to load step info from XML", e );
    }
  }

  @Override
  public String getXML() {
    StringBuffer retval = new StringBuffer( 800 );

    retval.append( "    " ).append( XMLHandler.addTagValue( "filename", filename ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "compression", compressionType ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "parquetVersion", parquetVersion ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "enableDictionary", enableDictionary ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "dictPageSize", dictPageSize ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "rowGroupSize", rowGroupSize ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "dataPageSize", dataPageSize ) );

    retval.append( "    <fields>" ).append( Const.CR );
    for ( int i = 0; i < outputFields.length; i++ ) {
      FormatInputOutputField field = outputFields[ i ];

      if ( field.getName() != null && field.getName().length() != 0 ) {
        retval.append( "      <field>" ).append( Const.CR );
        retval.append( "        " ).append( XMLHandler.addTagValue( "path", field.getPath() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "name", field.getName() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "type", field.getTypeDesc() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "nullable", field.getNullString() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "default", field.getIfNullValue() ) );
        retval.append( "      </field>" ).append( Const.CR );
      }
    }
    retval.append( "    </fields>" ).append( Const.CR );

    return retval.toString();
  }

  @Override
  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
    throws KettleException {
    try {
      filename = rep.getStepAttributeString( id_step, "filename" );
      compressionType = rep.getStepAttributeString( id_step, "compression" );
      parquetVersion = rep.getStepAttributeString( id_step, "parquetVersion" );
      enableDictionary = rep.getStepAttributeBoolean( id_step, "enableDictionary" );
      dictPageSize = rep.getStepAttributeString( id_step, "dictPageSize" );
      rowGroupSize = rep.getStepAttributeString( id_step, "rowGroupSize" );
      dataPageSize = rep.getStepAttributeString( id_step, "dataPageSize" );

      // using the "type" column to get the number of field rows because "type" is guaranteed not to be null.
      int nrfields = rep.countNrStepAttributes( id_step, "type" );

      List<FormatInputOutputField> parquetOutputFields = new ArrayList<>();
      for ( int i = 0; i < nrfields; i++ ) {
        FormatInputOutputField outputField = new FormatInputOutputField();

        outputField.setPath( rep.getStepAttributeString( id_step, i, "path" ) );
        outputField.setName( rep.getStepAttributeString( id_step, i, "name" ) );
        outputField.setType( rep.getStepAttributeString( id_step, i, "type" ) );
        outputField.setIfNullValue( rep.getStepAttributeString( id_step, i, "nullable" ) );
        outputField.setNullString( rep.getStepAttributeString( id_step, i, "default" ) );

        parquetOutputFields.add( outputField );
      }
      this.outputFields = parquetOutputFields.toArray( new FormatInputOutputField[ 0 ] );
    } catch ( Exception e ) {
      throw new KettleException( "Unexpected error reading step information from the repository", e );
    }
  }

  @Override
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
    throws KettleException {
    try {
      rep.saveStepAttribute( id_transformation, id_step, "filename", filename );
      rep.saveStepAttribute( id_transformation, id_step, "compression", compressionType );
      rep.saveStepAttribute( id_transformation, id_step, "parquetVersion", parquetVersion );
      rep.saveStepAttribute( id_transformation, id_step, "enableDictionary", enableDictionary );
      rep.saveStepAttribute( id_transformation, id_step, "dictPageSize", dictPageSize );
      rep.saveStepAttribute( id_transformation, id_step, "rowGroupSize", rowGroupSize );
      rep.saveStepAttribute( id_transformation, id_step, "dataPageSize", dataPageSize );
      for ( int i = 0; i < outputFields.length; i++ ) {
        FormatInputOutputField field = outputFields[ i ];

        rep.saveStepAttribute( id_transformation, id_step, i, "path", field.getPath() );
        rep.saveStepAttribute( id_transformation, id_step, i, "name", field.getName() );
        rep.saveStepAttribute( id_transformation, id_step, i, "type", field.getTypeDesc() );
        rep.saveStepAttribute( id_transformation, id_step, i, "nullable", field.getIfNullValue() );
        rep.saveStepAttribute( id_transformation, id_step, i, "default", field.getNullString() );
      }
    } catch ( Exception e ) {
      throw new KettleException( "Unable to save step information to the repository for id_step=" + id_step, e );
    }
  }

  @Override
  public void resolve() {
    if ( filename != null && !filename.isEmpty() ) {
      try {
        FileObject fileObject = KettleVFS.getFileObject( filename );
        if ( AliasedFileObject.isAliasedFile( fileObject ) ) {
          filename = ( (AliasedFileObject) fileObject ).getOriginalURIString();
        }
      } catch ( KettleFileException e ) {
        throw new RuntimeException( e );
      }
    }
  }

  public String constructOutputFilename() {
    String outputFileName = filename;
    if ( dateTimeFormat != null ) {
      outputFileName += new SimpleDateFormat( dateTimeFormat ).format( new Date() );
    } else {
      if ( dateInFilename ) {
        outputFileName += '_' + new SimpleDateFormat( "yyyyMMdd" ).format( new Date() );
      }
      if ( timeInFilename ) {
        outputFileName += '_' + new SimpleDateFormat( "HHmmss" ).format( new Date() );
      }
    }
    outputFileName += extension;
    return outputFileName;
  }

  public int getRowGroupSize( VariableSpace vspace ) {
    return parseReplace( rowGroupSize, vspace, str -> Integer.parseInt( str ), 0 );
  }

  protected <T> T parseReplace( String value, VariableSpace vspace, Function<String, T> parser, T defaultValue ) {
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

  public EncodingType getEncodingType( VariableSpace vspace ) {
    return enableDictionary ? EncodingType.DICTIONARY : EncodingType.PLAIN;
  }

  public void setEncodingType( String encoding ) {
    enableDictionary = encoding != null ? encoding.startsWith( "D" ) : false;
  }

  public String getEncodingType() {
    return enableDictionary ? EncodingType.DICTIONARY.uiName : EncodingType.PLAIN.uiName;
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
    String[] names = new String[ objects.length ];
    int i = 0;
    for ( T obj : objects ) {
      names[ i++ ] = obj.toString();
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

