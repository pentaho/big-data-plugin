/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2017 by Hitachi Vantara : http://www.pentaho.com
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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.apache.commons.vfs2.FileObject;
import org.pentaho.big.data.kettle.plugins.formats.avro.AvroFormatInputOutputField;
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

  @Injection( name = "FILENAME" ) private String filename;

  @InjectionDeep
  private List<AvroFormatInputOutputField> outputFields = new ArrayList<AvroFormatInputOutputField>();

  @Injection( name = "OPTIONS_COMPRESSION" ) protected String compressionType;
  @Injection( name = "SCHEMA_FILENAME" ) protected String schemaFilename;
  @Injection( name = "SCHEMA_NAMESPACE" ) protected String namespace;
  @Injection( name = "SCHEMA_RECORD_NAME" ) protected String recordName;
  @Injection( name = "SCHEMA_DOC_VALUE" ) protected String docValue;

  @Override
  public void setDefault() {
    // TODO Auto-generated method stub
  }

  public String getFilename() {
    return filename;
  }

  public void setFilename( String filename ) {
    this.filename = filename;
  }

  public List<AvroFormatInputOutputField> getOutputFields() {
    return outputFields;
  }

  public void setOutputFields( List<AvroFormatInputOutputField> outputFields ) {
    this.outputFields = outputFields;
  }

  @Override
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    readData( stepnode, metaStore );
  }

  private void readData( Node stepnode, IMetaStore metastore ) throws KettleXMLException {
    try {
      filename = XMLHandler.getTagValue( stepnode, "filename" );
      Node fields = XMLHandler.getSubNode( stepnode, "fields" );
      int nrfields = XMLHandler.countNodes( fields, "field" );
      List<AvroFormatInputOutputField> avroOutputFields = new ArrayList<>();
      for ( int i = 0; i < nrfields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );
        AvroFormatInputOutputField outputField = new AvroFormatInputOutputField();
        outputField.setPath( XMLHandler.getTagValue( fnode, "path" ) );
        outputField.setName( XMLHandler.getTagValue( fnode, "name" ) );
        outputField.setType( XMLHandler.getTagValue( fnode, "type" ) );
        outputField.setNullString( XMLHandler.getTagValue( fnode, "nullable" ) );
        outputField.setIfNullValue( XMLHandler.getTagValue( fnode, "default" )  );
        avroOutputFields.add( outputField );
      }
      this.outputFields = avroOutputFields;

      compressionType = XMLHandler.getTagValue( stepnode, FieldNames.COMPRESSION );
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

    retval.append( "    <fields>" ).append( Const.CR );
    for ( int i = 0; i < outputFields.size(); i++ ) {
      AvroFormatInputOutputField field = outputFields.get( i );

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

    retval.append( INDENT ).append( XMLHandler.addTagValue( FieldNames.COMPRESSION, compressionType ) );
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

      // using the "type" column to get the number of field rows because "type" is guaranteed not to be null.
      int nrfields = rep.countNrStepAttributes( id_step, "type" );

      List<AvroFormatInputOutputField> avroOutputFields = new ArrayList<>();
      for ( int i = 0; i < nrfields; i++ ) {
        AvroFormatInputOutputField outputField = new AvroFormatInputOutputField();

        outputField.setPath( rep.getStepAttributeString( id_step, i, "path" ) );
        outputField.setName( rep.getStepAttributeString( id_step, i, "name" ) );
        outputField.setType( rep.getStepAttributeString( id_step, i, "type" ) );
        outputField.setNullString( rep.getStepAttributeString( id_step, i, "nullable" ) );
        outputField.setIfNullValue( rep.getStepAttributeString( id_step, i, "default" ) );

        avroOutputFields.add( outputField );
      }
      this.outputFields = avroOutputFields;
      compressionType = rep.getStepAttributeString( id_step, FieldNames.COMPRESSION );
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
      for ( int i = 0; i < outputFields.size(); i++ ) {
        AvroFormatInputOutputField field = outputFields.get( i );

        rep.saveStepAttribute( id_transformation, id_step, i, "path", field.getPath() );
        rep.saveStepAttribute( id_transformation, id_step, i, "name", field.getName() );
        rep.saveStepAttribute( id_transformation, id_step, i, "type", field.getTypeDesc() );
        rep.saveStepAttribute( id_transformation, id_step, i, "nullable", field.getNullString() );
        rep.saveStepAttribute( id_transformation, id_step, i, "default", field.getIfNullValue() );
      }
      super.saveRep( rep, metaStore, id_transformation, id_step );
      rep.saveStepAttribute( id_transformation, id_step, FieldNames.COMPRESSION, compressionType );
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
    compressionType = StringUtil.isVariable( value ) ? value : parseFromToString( value, CompressionType.values(), null ).name();
  }

  public CompressionType getCompressionType( VariableSpace vspace ) {
    return parseReplace( compressionType, vspace, str -> CompressionType.valueOf( str ), CompressionType.NONE );
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

  private static String getMsg( String key ) {
    return BaseMessages.getString( PKG, key );
  }

  protected static class FieldNames {
    public static final String COMPRESSION = "compression";
    public static final String SCHEMA_FILENAME = "schemaFilename";
    public static final String RECORD_NAME = "recordName";
    public static final String DOC_VALUE = "docValue";
    public static final String NAMESPACE = "namespace";
  }

}
