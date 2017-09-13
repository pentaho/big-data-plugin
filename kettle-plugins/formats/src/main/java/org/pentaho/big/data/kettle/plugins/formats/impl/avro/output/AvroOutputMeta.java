/*******************************************************************************
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

package org.pentaho.big.data.kettle.plugins.formats.impl.avro.output;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceLocator;
import org.pentaho.big.data.kettle.plugins.formats.avro.output.AvroOutputMetaBase;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.List;
import java.util.function.Function;

@Step( id = "AvroOutput", image = "AO.svg", name = "AvroOutput.Name", description = "AvroOutput.Description",
    categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.BigData",
    documentationUrl = "http://wiki.pentaho.com/display/EAI/Avro+output",
    i18nPackageName = "org.pentaho.di.trans.steps.avro" )
public class AvroOutputMeta extends AvroOutputMetaBase {

  private static final Class<?> PKG = AvroOutputMeta.class;

  private final NamedClusterServiceLocator namedClusterServiceLocator;
  private final NamedClusterService namedClusterService;

  @Injection( name = FieldNames.COMPRESSION )
  private String compressionType;
  @Injection( name = FieldNames.SCHEMA_FILENAME ) private String schemaFilename;
  @Injection( name = FieldNames.NAMESPACE ) private String namespace;
  @Injection( name = FieldNames.RECORD_NAME ) private String recordName;
  @Injection( name = FieldNames.DOC_VALUE ) private String docValue;

  public AvroOutputMeta( NamedClusterServiceLocator namedClusterServiceLocator, NamedClusterService namedClusterService ) {
    this.namedClusterServiceLocator = namedClusterServiceLocator;
    this.namedClusterService = namedClusterService;
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {
    return new AvroOutput( stepMeta, stepDataInterface, copyNr, transMeta, trans, namedClusterServiceLocator );
  }

  @Override
  public StepDataInterface getStepData() {
    return new AvroOutputData();
  }

  @Override
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    super.loadXML( stepnode, databases, metaStore );
    compressionType = XMLHandler.getTagValue( stepnode, FieldNames.COMPRESSION );
    schemaFilename = XMLHandler.getTagValue( stepnode, FieldNames.SCHEMA_FILENAME );
    namespace = XMLHandler.getTagValue( stepnode, FieldNames.NAMESPACE );
    docValue = XMLHandler.getTagValue( stepnode, FieldNames.DOC_VALUE );
    recordName = XMLHandler.getTagValue( stepnode, FieldNames.RECORD_NAME );
  }

  @Override
  public String getXML() {
    StringBuffer retval = new StringBuffer( super.getXML() );
    final String INDENT = "    ";
    retval.append( INDENT ).append( XMLHandler.addTagValue( FieldNames.COMPRESSION, compressionType ) );
    retval.append( INDENT ).append( XMLHandler.addTagValue( FieldNames.SCHEMA_FILENAME, schemaFilename ) );
    retval.append( INDENT ).append( XMLHandler.addTagValue( FieldNames.NAMESPACE, namespace ) );
    retval.append( INDENT ).append( XMLHandler.addTagValue( FieldNames.DOC_VALUE, docValue ) );
    retval.append( INDENT ).append( XMLHandler.addTagValue( FieldNames.RECORD_NAME, recordName ) );
    return retval.toString();
  }

  @Override
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
    throws KettleException {
    super.saveRep( rep, metaStore, id_transformation, id_step );
    rep.saveStepAttribute( id_transformation, id_step, FieldNames.COMPRESSION, compressionType );
    rep.saveStepAttribute( id_transformation, id_step, FieldNames.SCHEMA_FILENAME, schemaFilename );
    rep.saveStepAttribute( id_transformation, id_step, FieldNames.NAMESPACE, namespace );
    rep.saveStepAttribute( id_transformation, id_step, FieldNames.DOC_VALUE, docValue );
    rep.saveStepAttribute( id_transformation, id_step, FieldNames.RECORD_NAME, recordName );
  }


  @Override
  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
      throws KettleException {
    super.readRep( rep, metaStore, id_step, databases );
    compressionType = rep.getStepAttributeString( id_step, FieldNames.COMPRESSION );
    schemaFilename = rep.getStepAttributeString( id_step, FieldNames.SCHEMA_FILENAME );
    namespace = rep.getStepAttributeString( id_step, FieldNames.NAMESPACE );
    docValue = rep.getStepAttributeString( id_step, FieldNames.DOC_VALUE );
    recordName = rep.getStepAttributeString( id_step, FieldNames.RECORD_NAME );
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

  public NamedCluster getNamedCluster() {
    return namedClusterService.getClusterTemplate();
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

  private static class FieldNames {
    public static final String COMPRESSION = "compression";
    public static final String SCHEMA_FILENAME = "schemaFilename";
    public static final String RECORD_NAME = "recordName";
    public static final String DOC_VALUE = "docValue";
    public static final String NAMESPACE = "namespace";
  }

  public static enum CompressionType {
    NONE( getMsg( "AvroOutput.CompressionType.NONE" ) ),
    DEFLATE( "Deflate" ),
    SNAPPY( "Snappy" );

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
}
