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

package org.pentaho.big.data.kettle.plugins.formats.avro.output;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.function.Function;

import org.apache.commons.vfs2.FileObject;
import org.pentaho.big.data.kettle.plugins.formats.avro.AvroTypeConverter;
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
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

/**
 * Avro output meta step without Hadoop-dependent classes. Required for read meta in the spark native code.
 *
 * @author Alexander Buloichik@epam.com>
 */
public abstract class AvroOutputMetaBase extends BaseStepMeta implements StepMetaInterface, ResolvableResource {

  private static final Class<?> PKG = AvroOutputMetaBase.class;

  private static final String DEFAULT = "default";

  @Injection( name = "FILENAME" ) private String filename;

  @InjectionDeep
  private List<AvroOutputField> outputFields = new ArrayList<AvroOutputField>();

  @Injection( name = "OPTIONS_DATE_IN_FILE_NAME" )
  protected boolean dateInFileName = false;

  @Injection( name = "OPTIONS_TIME_IN_FILE_NAME" )
  protected boolean timeInFileName = false;

  @Injection( name = "OPTIONS_DATE_FORMAT" )
  protected String dateTimeFormat = "";
  @Injection( name = "OPTIONS_COMPRESSION" ) protected String compressionType;
  @Injection( name = "SCHEMA_FILENAME" ) protected String schemaFilename;
  @Injection( name = "SCHEMA_NAMESPACE" ) protected String namespace;
  @Injection( name = "SCHEMA_RECORD_NAME" ) protected String recordName;
  @Injection( name = "SCHEMA_DOC_VALUE" ) protected String docValue;
  @Injection( name = "OVERRIDE_OUTPUT" )
  protected boolean overrideOutput;

  @Override
  public void setDefault() {
    // TODO Auto-generated method stub
  }

  public boolean isOverrideOutput() {
    return overrideOutput;
  }

  public void setOverrideOutput( boolean overrideOutput ) {
    this.overrideOutput = overrideOutput;
  }

  public String getFilename() {
    return filename;
  }

  public void setFilename( String filename ) {
    this.filename = filename;
  }

  public List<AvroOutputField> getOutputFields() {
    return outputFields;
  }

  public void setOutputFields( List<AvroOutputField> outputFields ) {
    this.outputFields = outputFields;
  }

  public boolean isDateInFileName() {
    return dateInFileName;
  }

  public void setDateInFileName( boolean dateInFileName ) {
    this.dateInFileName = dateInFileName;
  }

  public boolean isTimeInFileName() {
    return timeInFileName;
  }

  public void setTimeInFileName( boolean timeInFileName ) {
    this.timeInFileName = timeInFileName;
  }

  public String getDateTimeFormat() {
    return dateTimeFormat;
  }

  public void setDateTimeFormat( String dateTimeFormat ) {
    this.dateTimeFormat = dateTimeFormat;
  }

  @Override
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    readData( stepnode, metaStore );
  }

  private void readData( Node stepnode, IMetaStore metastore ) throws KettleXMLException {
    try {
      filename = XMLHandler.getTagValue( stepnode, "filename" );
      // Since we had override set to true in the previous release by default, we need to ensure that if the flag is
      // missing in the transformation xml, we set the override flag to true
      String override = XMLHandler.getTagValue( stepnode, FieldNames.OVERRIDE_OUTPUT );
      if ( override != null && override.length() > 0 ) {
        overrideOutput = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, FieldNames.OVERRIDE_OUTPUT ) );
      } else {
        overrideOutput = true;
      }
      Node fields = XMLHandler.getSubNode( stepnode, "fields" );
      int nrfields = XMLHandler.countNodes( fields, "field" );
      List<AvroOutputField> avroOutputFields = new ArrayList<>();
      for ( int i = 0; i < nrfields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );
        AvroOutputField outputField = new AvroOutputField();
        outputField.setFormatFieldName( XMLHandler.getTagValue( fnode, "path" ) );
        outputField.setPentahoFieldName( XMLHandler.getTagValue( fnode, "name" ) );
        outputField.setFormatType( AvroTypeConverter.convertToAvroType(  XMLHandler.getTagValue( fnode, "type" ) ) );
        outputField.setPrecision( XMLHandler.getTagValue( fnode, "precision" ) );
        outputField.setScale( XMLHandler.getTagValue( fnode, "scale" ) );
        outputField.setAllowNull( XMLHandler.getTagValue( fnode, "nullable" ) );
        outputField.setDefaultValue( XMLHandler.getTagValue( fnode, "default" )  );
        avroOutputFields.add( outputField );
      }
      this.outputFields = avroOutputFields;

      compressionType = XMLHandler.getTagValue( stepnode, FieldNames.COMPRESSION );
      dateTimeFormat = XMLHandler.getTagValue( stepnode, FieldNames.DATE_FORMAT );
      dateInFileName = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, FieldNames.DATE_IN_FILE_NAME ) );
      timeInFileName = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, FieldNames.TIME_IN_FILE_NAME ) );
      schemaFilename = XMLHandler.getTagValue( stepnode, FieldNames.SCHEMA_FILENAME );
      namespace = XMLHandler.getTagValue( stepnode, FieldNames.NAMESPACE );
      docValue = XMLHandler.getTagValue( stepnode, FieldNames.DOC_VALUE );
      recordName = XMLHandler.getTagValue( stepnode, FieldNames.RECORD_NAME );

    } catch ( Exception e ) {
      throw new KettleXMLException( "Unable to load step info from XML", e );
    }
  }

  @Override
  public String getXML() {
    StringBuffer retval = new StringBuffer( 800 );
    final String INDENT = "    ";

    retval.append( INDENT ).append( XMLHandler.addTagValue( "filename", filename ) );
    retval.append( INDENT ).append( XMLHandler.addTagValue( FieldNames.OVERRIDE_OUTPUT, overrideOutput ) );

    retval.append( "    <fields>" ).append( Const.CR );
    for ( int i = 0; i < outputFields.size(); i++ ) {
      AvroOutputField field = outputFields.get( i );

      if ( field.getPentahoFieldName() != null && field.getPentahoFieldName().length() != 0 ) {
        retval.append( "      <field>" ).append( Const.CR );
        retval.append( "        " ).append( XMLHandler.addTagValue( "path", field.getFormatFieldName() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "name", field.getPentahoFieldName() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "type", field.getAvroType().getId() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "precision", field.getPrecision() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "scale", field.getScale() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "nullable", field.getAllowNull() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "default", field.getDefaultValue() ) );
        retval.append( "      </field>" ).append( Const.CR );
      }
    }
    retval.append( "    </fields>" ).append( Const.CR );

    retval.append( INDENT ).append( XMLHandler.addTagValue( FieldNames.COMPRESSION, compressionType ) );
    retval.append( INDENT ).append( XMLHandler.addTagValue( FieldNames.DATE_FORMAT, dateTimeFormat ) );
    retval.append( INDENT ).append( XMLHandler.addTagValue( FieldNames.DATE_IN_FILE_NAME, dateInFileName ) );
    retval.append( INDENT ).append( XMLHandler.addTagValue( FieldNames.TIME_IN_FILE_NAME, timeInFileName ) );
    retval.append( INDENT ).append( XMLHandler.addTagValue( FieldNames.SCHEMA_FILENAME, schemaFilename ) );
    retval.append( INDENT ).append( XMLHandler.addTagValue( FieldNames.NAMESPACE, namespace ) );
    retval.append( INDENT ).append( XMLHandler.addTagValue( FieldNames.DOC_VALUE, docValue ) );
    retval.append( INDENT ).append( XMLHandler.addTagValue( FieldNames.RECORD_NAME, recordName ) );

    return retval.toString();
  }

  @Override
  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
      throws KettleException {
    try {
      filename = rep.getStepAttributeString( id_step, "filename" );
      // Since we had override set to true in the previous release by default, we need to ensure that if the flag is
      // missing in the transformation xml, we set the override flag to true
      String override = rep.getStepAttributeString( id_step, FieldNames.OVERRIDE_OUTPUT );
      if ( override != null && override.length() > 0 ) {
        overrideOutput = rep.getStepAttributeBoolean( id_step, FieldNames.OVERRIDE_OUTPUT );
      } else {
        overrideOutput = true;
      }
      // using the "type" column to get the number of field rows because "type" is guaranteed not to be null.
      int nrfields = rep.countNrStepAttributes( id_step, "type" );

      List<AvroOutputField> avroOutputFields = new ArrayList<>();
      for ( int i = 0; i < nrfields; i++ ) {
        AvroOutputField outputField = new AvroOutputField();

        outputField.setFormatFieldName( rep.getStepAttributeString( id_step, i, "path" ) );
        outputField.setPentahoFieldName( rep.getStepAttributeString( id_step, i, "name" ) );
        outputField.setFormatType( AvroTypeConverter.convertToAvroType( rep.getStepAttributeString( id_step, i, "type" ) ) );
        outputField.setPrecision( rep.getStepAttributeString( id_step, i, "precision" ) );
        outputField.setScale( rep.getStepAttributeString( id_step, i, "scale" ) );
        outputField.setAllowNull( rep.getStepAttributeString( id_step, i, "nullable" ) );
        outputField.setDefaultValue( rep.getStepAttributeString( id_step, i, "default" ) );

        avroOutputFields.add( outputField );
      }
      this.outputFields = avroOutputFields;
      compressionType = rep.getStepAttributeString( id_step, FieldNames.COMPRESSION );
      dateTimeFormat = rep.getStepAttributeString( id_step, FieldNames.DATE_FORMAT );
      dateInFileName = rep.getStepAttributeBoolean( id_step, FieldNames.DATE_IN_FILE_NAME );
      timeInFileName = rep.getStepAttributeBoolean( id_step, FieldNames.TIME_IN_FILE_NAME );
      schemaFilename = rep.getStepAttributeString( id_step, FieldNames.SCHEMA_FILENAME );
      namespace = rep.getStepAttributeString( id_step, FieldNames.NAMESPACE );
      docValue = rep.getStepAttributeString( id_step, FieldNames.DOC_VALUE );
      recordName = rep.getStepAttributeString( id_step, FieldNames.RECORD_NAME );
    } catch ( Exception e ) {
      throw new KettleException( "Unexpected error reading step information from the repository", e );
    }
  }

  @Override
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
      throws KettleException {
    try {
      rep.saveStepAttribute( id_transformation, id_step, "filename", filename );
      rep.saveStepAttribute( id_transformation, id_step, FieldNames.OVERRIDE_OUTPUT, overrideOutput );

      for ( int i = 0; i < outputFields.size(); i++ ) {
        AvroOutputField field = outputFields.get( i );

        rep.saveStepAttribute( id_transformation, id_step, i, "path", field.getFormatFieldName() );
        rep.saveStepAttribute( id_transformation, id_step, i, "name", field.getPentahoFieldName() );
        rep.saveStepAttribute( id_transformation, id_step, i, "type", field.getAvroType().getId() );
        rep.saveStepAttribute( id_transformation, id_step, i, "precision", field.getPrecision() );
        rep.saveStepAttribute( id_transformation, id_step, i, "scale", field.getScale() );
        rep.saveStepAttribute( id_transformation, id_step, i, "nullable", Boolean.toString( field.getAllowNull() ) );
        String defaultValue = StringUtil.isEmpty( field.getDefaultValue() ) ? null : field.getDefaultValue();
        rep.saveStepAttribute( id_transformation, id_step, i, DEFAULT, defaultValue );
      }
      super.saveRep( rep, metaStore, id_transformation, id_step );
      rep.saveStepAttribute( id_transformation, id_step, FieldNames.COMPRESSION, compressionType );
      rep.saveStepAttribute( id_transformation, id_step, FieldNames.DATE_FORMAT, dateTimeFormat );
      rep.saveStepAttribute( id_transformation, id_step, FieldNames.DATE_IN_FILE_NAME, dateInFileName );
      rep.saveStepAttribute( id_transformation, id_step, FieldNames.TIME_IN_FILE_NAME, timeInFileName );
      rep.saveStepAttribute( id_transformation, id_step, FieldNames.SCHEMA_FILENAME, schemaFilename );
      rep.saveStepAttribute( id_transformation, id_step, FieldNames.NAMESPACE, namespace );
      rep.saveStepAttribute( id_transformation, id_step, FieldNames.DOC_VALUE, docValue );
      rep.saveStepAttribute( id_transformation, id_step, FieldNames.RECORD_NAME, recordName );

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

    if ( schemaFilename != null && !schemaFilename.isEmpty() ) {
      try {
        String realSchemaFilename = getParentStepMeta().getParentTransMeta().environmentSubstitute( schemaFilename );
        FileObject fileObject = KettleVFS.getFileObject( realSchemaFilename );
        if ( AliasedFileObject.isAliasedFile( fileObject ) ) {
          schemaFilename = ( (AliasedFileObject) fileObject ).getOriginalURIString();
        }
      } catch ( KettleFileException e ) {
        throw new RuntimeException( e );
      }
    }
  }

  public String getSchemaFilename() {
    return schemaFilename;
  }

  public void setSchemaFilename( String schemaFilename ) {
    this.schemaFilename = schemaFilename;
  }

  public String getNamespace() {
    return namespace;
  }

  public void setNamespace( String namespace ) {
    this.namespace = namespace;
  }

  public String getRecordName() {
    return recordName;
  }

  public void setRecordName( String recordName ) {
    this.recordName = recordName;
  }

  public String getDocValue() {
    return docValue;
  }

  public void setDocValue( String docValue ) {
    this.docValue = docValue;
  }

  public String getCompressionType() {
    return StringUtil.isVariable( compressionType ) ? compressionType : getCompressionType( null ).toString();
  }

  public void setCompressionType( String value ) {
    compressionType = StringUtil.isVariable( value ) ? value : parseFromToString( value, CompressionType.values(), CompressionType.NONE ).name();
  }

  public CompressionType getCompressionType( VariableSpace vspace ) {
    return parseReplace( compressionType, vspace, str -> findCompressionType( str ), CompressionType.NONE );
  }

  private  CompressionType findCompressionType( String str ) {
    try {
      return CompressionType.valueOf( str );
    } catch ( Throwable th ) {
      return parseFromToString( str, CompressionType.values(), CompressionType.NONE );
    }
  }

  public String[] getCompressionTypes() {
    return getStrings( CompressionType.values() );
  }

  public static enum CompressionType {
    NONE( getMsg( "AvroOutput.CompressionType.NONE" ) ),
    DEFLATE( getMsg( "AvroOutput.CompressionType.DEFLATE" ) ),
    SNAPPY( getMsg( "AvroOutput.CompressionType.SNAPPY" ) );

    private final String name;

    private CompressionType( String name ) {
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

  private  <T> T parseReplace( String value, VariableSpace vspace, Function<String, T> parser, T defaultValue ) {
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

  public String constructOutputFilename( String file ) {
    int endIndex = file.lastIndexOf( "." );
    String name = endIndex > 0 ? file.substring( 0, endIndex ) : file;
    String extension = endIndex <= 0 ? "" : file.substring( endIndex, file.length() );

    if ( dateTimeFormat != null && !dateTimeFormat.isEmpty() ) {
      String dateTimeFormatPattern = getParentStepMeta().getParentTransMeta().environmentSubstitute( dateTimeFormat );
      name += new SimpleDateFormat( dateTimeFormatPattern ).format( new Date() );
    } else {
      if ( dateInFileName ) {
        name += '_' + new SimpleDateFormat( "yyyyMMdd" ).format( new Date() );
      }
      if ( timeInFileName ) {
        name += '_' + new SimpleDateFormat( "HHmmss" ).format( new Date() );
      }
    }
    return name + extension;
  }

  private static String getMsg( String key ) {
    return BaseMessages.getString( PKG, key );
  }

  protected static class FieldNames {
    public static final String COMPRESSION = "compression";
    public static final String SCHEMA_FILENAME = "schemaFilename";
    public static final String OVERRIDE_OUTPUT = "overrideOutput";
    public static final String RECORD_NAME = "recordName";
    public static final String DOC_VALUE = "docValue";
    public static final String NAMESPACE = "namespace";
    public static final String DATE_IN_FILE_NAME = "dateInFileName";
    public static final String TIME_IN_FILE_NAME = "timeInFileName";
    public static final String DATE_FORMAT = "dateTimeFormat";
  }

}
