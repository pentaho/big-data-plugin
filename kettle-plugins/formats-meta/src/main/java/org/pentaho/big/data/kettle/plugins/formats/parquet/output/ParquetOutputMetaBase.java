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

package org.pentaho.big.data.kettle.plugins.formats.parquet.output;

import org.apache.commons.vfs2.FileObject;
import org.pentaho.big.data.kettle.plugins.formats.parquet.ParquetTypeConverter;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionDeep;
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
import org.pentaho.hadoop.shim.api.format.ParquetSpec;
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

  @Injection( name = "FILENAME", group = "FILENAME_LINES" )
  public String filename;

  @InjectionDeep
  private List<ParquetOutputField> outputFields = new ArrayList<ParquetOutputField>();

  @Override
  public void setDefault() {
    outputFields = new ArrayList<ParquetOutputField>();
    dictPageSize = String.valueOf( 1024 );
    extension = "parquet";
  }

  public String getFilename() {
    return filename;
  }

  public void setFilename( String filename ) {
    this.filename = filename;
  }

  public boolean isEnableDictionary() {
    return enableDictionary;
  }

  public void setEnableDictionary( boolean enableDictionary ) {
    this.enableDictionary = enableDictionary;
  }

  public boolean isOverrideOutput() {
    return overrideOutput;
  }

  public void setOverrideOutput( boolean overrideOutput ) {
    this.overrideOutput = overrideOutput;
  }

  public boolean isDateInFilename() {
    return dateInFilename;
  }

  public void setDateInFilename( boolean dateInFilename ) {
    this.dateInFilename = dateInFilename;
  }

  public boolean isTimeInFilename() {
    return timeInFilename;
  }

  public void setTimeInFilename( boolean timeInFilename ) {
    this.timeInFilename = timeInFilename;
  }

  public String getDateTimeFormat() {
    return dateTimeFormat;
  }

  public void setDateTimeFormat( String dateTimeFormat ) {
    this.dateTimeFormat = dateTimeFormat;
  }

  public String getExtension() {
    return extension;
  }

  public void setExtension( String extension ) {
    this.extension = extension;
  }

  public List<ParquetOutputField> getOutputFields() {
    return outputFields;
  }

  public void setOutputFields( List<ParquetOutputField> outputFields ) {
    this.outputFields = outputFields;
  }

  @Override
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    readData( stepnode, metaStore );
  }

  private void readData( Node stepnode, IMetaStore metastore ) throws KettleXMLException {
    try {
      filename = XMLHandler.getTagValue( stepnode, "filename" );
      overrideOutput = "Y".equalsIgnoreCase( ( XMLHandler.getTagValue( stepnode, "overrideOutput" ) ) );
      enableDictionary = "Y".equalsIgnoreCase( ( XMLHandler.getTagValue( stepnode, "enableDictionary" ) ) );
      compressionType = XMLHandler.getTagValue( stepnode, "compression" );
      parquetVersion = XMLHandler.getTagValue( stepnode, "parquetVersion" );
      rowGroupSize = XMLHandler.getTagValue( stepnode, "rowGroupSize" );
      dataPageSize = XMLHandler.getTagValue( stepnode, "dataPageSize" );
      dictPageSize = XMLHandler.getTagValue( stepnode, "dictPageSize" );
      extension = XMLHandler.getTagValue( stepnode, "extension" );
      dateInFilename = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "dateInFilename" ) );
      timeInFilename = "Y".equalsIgnoreCase( ( XMLHandler.getTagValue( stepnode, "timeInFilename" ) ) );
      dateTimeFormat = XMLHandler.getTagValue( stepnode, "dateTimeFormat" );

      Node fields = XMLHandler.getSubNode( stepnode, "fields" );
      int nrfields = XMLHandler.countNodes( fields, "field" );
      List<ParquetOutputField> parquetOutputFields = new ArrayList<>();
      for ( int i = 0; i < nrfields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );
        ParquetOutputField outputField = new ParquetOutputField();
        outputField.setFormatFieldName( XMLHandler.getTagValue( fnode, "path" ) );
        outputField.setPentahoFieldName( XMLHandler.getTagValue( fnode, "name" ) );
        int parquetTypeId = getParquetTypeId( XMLHandler.getTagValue( fnode, "type" ) );
        outputField.setFormatType( parquetTypeId );
        outputField.setPrecision( XMLHandler.getTagValue( fnode, "precision" ) );
        outputField.setScale( XMLHandler.getTagValue( fnode, "scale" ) );
        outputField.setAllowNull( "Y".equalsIgnoreCase( XMLHandler.getTagValue( fnode, "nullable" ) ) );
        outputField.setDefaultValue( XMLHandler.getTagValue( fnode, "default" ) );
        parquetOutputFields.add( outputField );
      }
      this.outputFields = parquetOutputFields;
    } catch ( Exception e ) {
      throw new KettleXMLException( "Unable to load step info from XML", e );
    }
  }

  @Override
  public String getXML() {
    StringBuffer retval = new StringBuffer( 800 );

    if ( parentStepMeta != null && parentStepMeta.getParentTransMeta() != null ) {
      parentStepMeta.getParentTransMeta().getNamedClusterEmbedManager().registerUrl( filename );
    }

    retval.append( "    " ).append( XMLHandler.addTagValue( "filename", filename ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "overrideOutput", overrideOutput ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "compression", compressionType ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "parquetVersion", parquetVersion ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "enableDictionary", enableDictionary ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "dictPageSize", dictPageSize ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "rowGroupSize", rowGroupSize ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "dataPageSize", dataPageSize ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "extension", extension ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "dateInFilename", dateInFilename ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "timeInFilename", timeInFilename ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "dateTimeFormat", dateTimeFormat ) );

    retval.append( "    <fields>" ).append( Const.CR );
    for ( int i = 0; i < outputFields.size(); i++ ) {
      ParquetOutputField field = outputFields.get( i );

      if ( field.getPentahoFieldName() != null && field.getPentahoFieldName().length() != 0 ) {
        retval.append( "      <field>" ).append( Const.CR );
        retval.append( "        " ).append( XMLHandler.addTagValue( "path", field.getFormatFieldName() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "name", field.getPentahoFieldName() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "type", field.getFormatType() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "precision", field.getPrecision() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "scale", field.getScale() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "nullable", field.getAllowNull() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "default", field.getDefaultValue() ) );
        retval.append( "      </field>" ).append( Const.CR );
      }
    }
    retval.append( "    </fields>" ).append( Const.CR );

    return retval.toString();
  }

  private int getParquetTypeId( String savedType ) {
    int parquetTypeId = 0;
    try {
      parquetTypeId = Integer.parseInt( savedType );
    } catch ( NumberFormatException e ) {
      String parquetTypeName = ParquetTypeConverter.convertToParquetType( savedType );
      for ( ParquetSpec.DataType parquetType : ParquetSpec.DataType.values() ) {
        if ( parquetType.getName().equals( parquetTypeName ) ) {
          parquetTypeId = parquetType.getId();
          break;
        }
      }
    }
    return parquetTypeId;
  }

  @Override
  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
    throws KettleException {
    try {
      filename = rep.getStepAttributeString( id_step, "filename" );
      overrideOutput = rep.getStepAttributeBoolean( id_step, "overrideOutput" );
      compressionType = rep.getStepAttributeString( id_step, "compression" );
      parquetVersion = rep.getStepAttributeString( id_step, "parquetVersion" );
      enableDictionary = rep.getStepAttributeBoolean( id_step, "enableDictionary" );
      dictPageSize = rep.getStepAttributeString( id_step, "dictPageSize" );
      rowGroupSize = rep.getStepAttributeString( id_step, "rowGroupSize" );
      dataPageSize = rep.getStepAttributeString( id_step, "dataPageSize" );
      extension = rep.getStepAttributeString( id_step, "extension" );
      dateInFilename = rep.getStepAttributeBoolean( id_step, "dateInFilename" );
      timeInFilename = rep.getStepAttributeBoolean( id_step, "timeInFilename" );
      dateTimeFormat = rep.getStepAttributeString( id_step, "dateTimeFormat" );

      // using the "type" column to get the number of field rows because "type" is guaranteed not to be null.
      int nrfields = rep.countNrStepAttributes( id_step, "type" );

      List<ParquetOutputField> parquetOutputFields = new ArrayList<>();
      for ( int i = 0; i < nrfields; i++ ) {
        ParquetOutputField outputField = new ParquetOutputField();
        outputField.setFormatFieldName( rep.getStepAttributeString( id_step, i, "path" ) );
        outputField.setPentahoFieldName( rep.getStepAttributeString( id_step, i, "name" ) );
        int parquetTypeId = getParquetTypeId( rep.getStepAttributeString( id_step, i, "type" ) );
        outputField.setFormatType( parquetTypeId );
        outputField.setPrecision( rep.getStepAttributeString( id_step, i, "precision" ) );
        outputField.setScale( rep.getStepAttributeString( id_step, i, "scale" ) );
        outputField.setAllowNull( rep.getStepAttributeBoolean( id_step, i, "nullable" ) );
        outputField.setDefaultValue( rep.getStepAttributeString( id_step, i, "default" ) );
        parquetOutputFields.add( outputField );
      }
      this.outputFields = parquetOutputFields;
    } catch ( Exception e ) {
      throw new KettleException( "Unexpected error reading step information from the repository", e );
    }
  }

  @Override
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
    throws KettleException {
    try {
      rep.saveStepAttribute( id_transformation, id_step, "filename", filename );
      rep.saveStepAttribute( id_transformation, id_step, "overrideOutput", overrideOutput );
      rep.saveStepAttribute( id_transformation, id_step, "compression", compressionType );
      rep.saveStepAttribute( id_transformation, id_step, "parquetVersion", parquetVersion );
      rep.saveStepAttribute( id_transformation, id_step, "enableDictionary", enableDictionary );
      rep.saveStepAttribute( id_transformation, id_step, "dictPageSize", dictPageSize );
      rep.saveStepAttribute( id_transformation, id_step, "rowGroupSize", rowGroupSize );
      rep.saveStepAttribute( id_transformation, id_step, "dataPageSize", dataPageSize );
      rep.saveStepAttribute( id_transformation, id_step, "extension", extension );
      rep.saveStepAttribute( id_transformation, id_step, "dateInFilename", dateInFilename );
      rep.saveStepAttribute( id_transformation, id_step, "timeInFilename", timeInFilename );
      rep.saveStepAttribute( id_transformation, id_step, "dateTimeFormat", dateTimeFormat );
      for ( int i = 0; i < outputFields.size(); i++ ) {
        ParquetOutputField field = outputFields.get( i );
        rep.saveStepAttribute( id_transformation, id_step, i, "path", field.getFormatFieldName() );
        rep.saveStepAttribute( id_transformation, id_step, i, "name", field.getPentahoFieldName() );
        rep.saveStepAttribute( id_transformation, id_step, i, "type", field.getFormatType() );
        rep.saveStepAttribute( id_transformation, id_step, i, "precision", field.getPrecision() );
        rep.saveStepAttribute( id_transformation, id_step, i, "scale", field.getScale() );
        rep.saveStepAttribute( id_transformation, id_step, i, "nullable", field.getAllowNull() );
        rep.saveStepAttribute( id_transformation, id_step, i, "default", field.getDefaultValue() );
      }
    } catch ( Exception e ) {
      throw new KettleException( "Unable to save step information to the repository for id_step=" + id_step, e );
    }
  }

  @Override
  public void resolve() {
    if ( filename != null && !filename.isEmpty() ) {
      try {
        String realFileName = getParentStepMeta().getParentTransMeta().environmentSubstitute( filename );
        FileObject fileObject = KettleVFS.getFileObject( realFileName );
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
    if ( dateTimeFormat != null && !dateTimeFormat.isEmpty() ) {
      String dateTimeFormatPattern = getParentStepMeta().getParentTransMeta().environmentSubstitute( dateTimeFormat );
      outputFileName += new SimpleDateFormat( dateTimeFormatPattern ).format( new Date() );
    } else {
      if ( dateInFilename ) {
        outputFileName += '_' + new SimpleDateFormat( "yyyyMMdd" ).format( new Date() );
      }
      if ( timeInFilename ) {
        outputFileName += '_' + new SimpleDateFormat( "HHmmss" ).format( new Date() );
      }
    }
    if ( extension != null && !extension.isEmpty() ) {
      outputFileName += '.' + extension;
    }
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
      StringUtil.isVariable( value ) ? value : parseFromToString( value, CompressionType.values(), CompressionType.NONE ).name();
  }

  public CompressionType getCompressionType( VariableSpace vspace ) {
    return parseReplace( compressionType, vspace, str -> findCompressionType( str ), CompressionType.NONE );
  }

  public String getParquetVersion() {
    return StringUtil.isVariable( parquetVersion ) ? parquetVersion : getParquetVersion( null ).toString();
  }

  public void setParquetVersion( String value ) {
    parquetVersion =
      StringUtil.isVariable( value ) ? value : parseFromToString( value, ParquetVersion.values(), CompressionType.NONE ).name();
  }

  public ParquetVersion getParquetVersion( VariableSpace vspace ) {
    return parseReplace( parquetVersion, vspace, str -> findParquetVersion( str ), ParquetVersion.PARQUET_1 );
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
    return parseReplace( dictPageSize, vspace, s -> Integer.parseInt( s ), 0 );
  }

  public String getDictPageSize() {
    return dictPageSize;
  }

  public void setDictPageSize( String dictPageSize ) {
    this.dictPageSize = dictPageSize;
  }

  public String[] getCompressionTypes() {
    return getStrings( CompressionType.values() );
  }

  public String[] getVersionTypes() {
    return getStrings( ParquetVersion.values() );
  }

  private  CompressionType findCompressionType( String str ) {
    try {
      return CompressionType.valueOf( str );
    } catch ( Throwable th ) {
      return parseFromToString( str, CompressionType.values(), CompressionType.NONE );
    }
  }

  private  ParquetVersion findParquetVersion( String str ) {
    try {
      return ParquetVersion.valueOf( str );
    } catch ( Throwable th ) {
      return parseFromToString( str, ParquetVersion.values(), ParquetVersion.PARQUET_1 );
    }
  }
  public static enum CompressionType {
    NONE( "None" ), SNAPPY( "Snappy" ), GZIP( "GZIP" );

    private final String uiName;

    private CompressionType( String name ) {
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

