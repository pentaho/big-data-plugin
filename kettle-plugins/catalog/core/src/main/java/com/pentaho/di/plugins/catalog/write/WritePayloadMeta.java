/*!
 * HITACHI VANTARA PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2020 Hitachi Vantara. All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Hitachi Vantara and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Hitachi Vantara and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Hitachi Vantara is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Hitachi Vantara,
 * explicitly covering such access.
 */

package com.pentaho.di.plugins.catalog.write;

import org.pentaho.big.data.kettle.plugins.formats.impl.NamedClusterResolver;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionDeep;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.steps.textfileoutput.TextFileField;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;
import org.w3c.dom.Node;

import java.util.List;

/**
 * Skeleton for PDI Step plugin.
 */
@Step( id = "Write Payload", image = "WritePayloadStep.svg", name = "Write Payload", description = "Write payload to "
  + "catelog", categoryDescription = "Catalog" )

@InjectionSupported( localizationPrefix = "Write.Injection.", groups = { "RESOURCE_FIELDS", "OUTPUT_FIELDS" } )
public class WritePayloadMeta extends BaseStepMeta implements StepMetaInterface {

  private static Class<?> catalogClass = WritePayload.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
  private static final String TAB_SELECTION_INDEX = "tabSelectionIndex";
  private static final String CONNECTION = "connection";
  private static final String VIRTUAL_FOLDERS = "virtualFolders";
  private static final String RESOURCE_NAME = "resourceName";
  private static final String FILE_FORMAT = "fileFormat";
  private static final String FILE_ALREADY_EXISTS_INDEX = "fileAlreadyExistsIndex";
  private static final String OPERATION_TYPE_INDEX = "operationTypeIndex";
  private static final String RESOURCE_ID = "resourceId";
  private static final String TAGS = "tags";
  private static final String DESCRIPTION = "description";
  private static final String PROP_A = "propA";
  private static final String PROP_B = "propB";
  private static final String PROP_C = "propC";
  private static final String PROP_D = "propD";
  private static final String WRITE_PAYLOAD_NODE_NAMESPACE = "WritePayload";

  public static final String DATA_RESOURCE = "Data Resource";
  public static final String DATA_SOURCE = "Data Source";

  private static final String FIELD = "field";
  private static final String FIELD_NAME  = "field_name";
  private static final String FIELD_PADDING = "        ";

  protected static final String[] DATA_TYPES = new String[] {
    DATA_RESOURCE, DATA_SOURCE
  };

  private final NamedClusterResolver namedClusterResolver;

  public NamedClusterResolver getNamedClusterResolver() {
    return namedClusterResolver;
  }

  @Injection( name = "TAB_SELECTION_INDEX" )
  private Integer tabSelectionIndex = 0;

  @Injection( name = "CONNECTION" )
  @SuppressWarnings( "java:S1845" )
  private String connection;

  @Injection( name = "VIRTUAL_FOLDERS" )
  private String virtualFolders = "";

  @Injection( name = "RESOURCE_NAME" )
  private String resourceName = "";

  @Injection( name = "FILE_FORMAT" )
  private String fileFormat = "";

  @Injection( name = "FILE_ALREADY_EXISTS_INDEX" )
  private Integer fileAlreadyExistsIndex = 0;

  @Injection( name = "OPERATION_TYPE_INDEX" )
  private Integer operationTypeIndex = 0;

  @Injection( name = "RESOURCE_ID" )
  private String resourceId = "";

  @Injection( name = "DESCRIPTION" )
  @SuppressWarnings( "java:S1845" )
  private String description = "";

  // TODO Fix when we get a real multi select
  @Injection( name = "TAGS" )
  @SuppressWarnings( "java:S1845" )
  private String tags = "";

  @Injection( name = "PROPERTY_A" )
  private String propA = "";

  @Injection( name = "PROPERTY_B" )
  private String propB = "";

  @Injection( name = "PROPERTY_C" )
  private String propC = "";

  @Injection( name = "PROPERTY_D" )
  private String propD = "";

  @InjectionDeep
  private TextFileField[] outputFields;

  public int getTabSelectionIndex() {
    return tabSelectionIndex;
  }

  public void setTabSelectionIndex( int tabSelectionIndex ) {
    this.tabSelectionIndex = tabSelectionIndex;
  }

  public String getConnection() {
    return connection;
  }

  public void setConnection( String connection ) {
    this.connection = connection;
  }

  public String getVirtualFolders() {
    return virtualFolders;
  }

  public void setVirtualFolders( String virtualFolders ) {
    this.virtualFolders =  virtualFolders;
  }

  public String getResourceName() {
    return resourceName;
  }

  public void setResourceName( String name ) {
    this.resourceName =  name;
  }

  public String getFileFormat() {
    return fileFormat;
  }

  public void setFileFormat( String fileFormat ) {
    this.fileFormat =  fileFormat;
  }

  public Integer getFileAlreadyExistsIndex() {
    return fileAlreadyExistsIndex;
  }

  public void setFileAlreadyExistsIndex( Integer fileAlreadyExistsIndex ) {
    this.fileAlreadyExistsIndex = fileAlreadyExistsIndex;
  }

  public Integer getOperationTypeIndex() {
    return operationTypeIndex;
  }

  public void setOperationTypeIndex( Integer operationTypeIndex ) {
    this.operationTypeIndex = operationTypeIndex;
  }

  public String getResourceId() {
    return resourceId;
  }

  public void setResourceId( String resourceId ) {
    this.resourceId =  resourceId;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription( String description ) {
    this.description =  description;
  }

  public String getSelectedTag() {
    return tags;
  }

  public void setSelectedTag( String tags ) {
    this.tags = tags;
  }

  public String getPropA() {
    return propA;
  }

  public void setPropA( String propA ) {
    this.propA = propA;
  }

  public String getPropB() {
    return propB;
  }

  public void setPropB( String propB ) {
    this.propB = propB;
  }

  public String getPropC() {
    return propC;
  }

  public void setPropC( String propC ) {
    this.propC = propC;
  }

  public String getPropD() {
    return propD;
  }

  public void setPropD( String propD ) {
    this.propD = propD;
  }

  public TextFileField[] getOutputFields() {
    return outputFields;
  }

  public void setOutputFields( TextFileField[] outputFields ) {
    this.outputFields = outputFields;
  }

  public WritePayloadMeta( NamedClusterResolver namedClusterResolver) {
    super(); // allocate BaseStepMeta
    outputFields = new TextFileField[ 0 ];
    this.namedClusterResolver = namedClusterResolver;
  }

  public void allocate( int nrfields ) {
    outputFields = new TextFileField[ nrfields ];
  }

  @Override
  @SuppressWarnings( "squid:S2975" )
  public Object clone() {
    WritePayloadMeta retval = (WritePayloadMeta) super.clone();
    int nrfields = outputFields.length;

    retval.allocate( nrfields );

    for ( int i = 0; i < nrfields; i++ ) {
      retval.outputFields[ i ] = (TextFileField) outputFields[ i ].clone();
    }
    return retval;
  }

  @Override
  public void setDefault() {
    tabSelectionIndex = 0;
    connection = "";
    virtualFolders = "";
    resourceName = "";
    fileFormat = "";
    fileAlreadyExistsIndex = 0;
    operationTypeIndex = 0;
    resourceId = "";
    tags = "";
    description = "";
    propA = "";
    propB = "";
    propC = "";
    propD = "";

    // Initialize output fields
    int i = 0;
    int nrfields = 0;

    allocate( nrfields );

    for ( i = 0; i < nrfields; i++ ) {
      outputFields[ i ] = new TextFileField();

      outputFields[ i ].setName( FIELD + i );
      outputFields[ i ].setType( "Number" );
      outputFields[ i ].setFormat( " 0,000,000.00;-0,000,000.00" );
      outputFields[ i ].setCurrencySymbol( "" );
      outputFields[ i ].setDecimalSymbol( "," );
      outputFields[ i ].setGroupingSymbol( "." );
      outputFields[ i ].setNullString( "" );
      outputFields[ i ].setLength( -1 );
      outputFields[ i ].setPrecision( -1 );
    }
  }

  public void getFields( RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, Repository repository, IMetaStore metaStore ) throws KettleStepException {
    // We do note have any fields to add to the Kettle stream at this time.
  }

  public void check( List<CheckResultInterface> remarks, TransMeta transMeta,
                     StepMeta stepMeta, RowMetaInterface prev, String[] input, String[] output,
                     RowMetaInterface info, VariableSpace space, Repository repository,
                     IMetaStore metaStore ) {
    if ( prev == null || prev.size() == 0 ) {
      remarks.add( new CheckResult( CheckResultInterface.TYPE_RESULT_WARNING,
        BaseMessages.getString( catalogClass, "SearchCatalogMeta.CheckResult.NotReceivingFields" ), stepMeta ) );
    } else {
      remarks.add( new CheckResult( CheckResultInterface.TYPE_RESULT_OK,
        BaseMessages.getString( catalogClass, "SearchCatalogMeta.CheckResult.StepRecevingData", prev.size() + "" ),
        stepMeta ) );
    }

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      remarks.add( new CheckResult( CheckResultInterface.TYPE_RESULT_OK,
        BaseMessages.getString( catalogClass, "SearchCatalogMeta.CheckResult.StepRecevingData2" ), stepMeta ) );
    } else {
      remarks.add( new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR,
        BaseMessages.getString( catalogClass, "SearchCatalogMeta.CheckResult.NoInputReceivedFromOtherSteps" ),
        stepMeta ) );
    }
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr,
                                Trans trans ) {
    return new WritePayload( stepMeta, stepDataInterface, cnr, tr, trans );
  }

  public StepDataInterface getStepData() {
    return new WritePayloadData();
  }

  public String getDialogClassName() {
    return "com.pentaho.di.plugins.catalog.write.WritePayloadDialog";
  }

  @Override
  /**
   * UNABLE TO USE BaseSerializingMeta because of the complexity of the Output fields
   */
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    super.loadXML( stepnode, databases, metaStore );
    setTabSelectionIndex( Integer.valueOf( XMLHandler.getTagValue( stepnode, WRITE_PAYLOAD_NODE_NAMESPACE + TAB_SELECTION_INDEX ) ) );
    setConnection( XMLHandler.getTagValue( stepnode, WRITE_PAYLOAD_NODE_NAMESPACE + CONNECTION ) );
    setVirtualFolders( XMLHandler.getTagValue( stepnode, WRITE_PAYLOAD_NODE_NAMESPACE + VIRTUAL_FOLDERS ) );
    setResourceName( XMLHandler.getTagValue( stepnode, WRITE_PAYLOAD_NODE_NAMESPACE + RESOURCE_NAME ) );
    setFileFormat( XMLHandler.getTagValue( stepnode, WRITE_PAYLOAD_NODE_NAMESPACE + FILE_FORMAT ) );
    fileAlreadyExistsIndex = Integer.valueOf( XMLHandler.getTagValue( stepnode, WRITE_PAYLOAD_NODE_NAMESPACE + FILE_ALREADY_EXISTS_INDEX ) );
    operationTypeIndex = Integer.valueOf( XMLHandler.getTagValue( stepnode, WRITE_PAYLOAD_NODE_NAMESPACE + OPERATION_TYPE_INDEX ) );
    setResourceId( XMLHandler.getTagValue( stepnode, WRITE_PAYLOAD_NODE_NAMESPACE + RESOURCE_ID ) );
    setSelectedTag( XMLHandler.getTagValue( stepnode, WRITE_PAYLOAD_NODE_NAMESPACE + TAGS )  );
    setDescription( XMLHandler.getTagValue( stepnode, WRITE_PAYLOAD_NODE_NAMESPACE + DESCRIPTION ) );
    setPropA( XMLHandler.getTagValue( stepnode, WRITE_PAYLOAD_NODE_NAMESPACE + PROP_A ) );
    setPropB( XMLHandler.getTagValue( stepnode, WRITE_PAYLOAD_NODE_NAMESPACE + PROP_B ) );
    setPropC( XMLHandler.getTagValue( stepnode, WRITE_PAYLOAD_NODE_NAMESPACE + PROP_C ) );
    setPropD( XMLHandler.getTagValue( stepnode, WRITE_PAYLOAD_NODE_NAMESPACE + PROP_D ) );

    // copied from TextFileOuputMeta.readData()
    Node fields = XMLHandler.getSubNode( stepnode, "fields" );
    int nrfields = XMLHandler.countNodes( fields, FIELD );
    this.allocate( nrfields );

    for ( int i = 0; i < nrfields; ++i ) {
      Node fnode = XMLHandler.getSubNodeByNr( fields, FIELD, i );
      this.outputFields[ i ] = new TextFileField();
      this.outputFields[ i ].setName( XMLHandler.getTagValue( fnode, "name" ) );
      this.outputFields[ i ].setType( XMLHandler.getTagValue( fnode, "type" ) );
      this.outputFields[ i ].setFormat( XMLHandler.getTagValue( fnode, "format" ) );
      this.outputFields[ i ].setCurrencySymbol( XMLHandler.getTagValue( fnode, "currency" ) );
      this.outputFields[ i ].setDecimalSymbol( XMLHandler.getTagValue( fnode, "decimal" ) );
      this.outputFields[ i ].setGroupingSymbol( XMLHandler.getTagValue( fnode, "group" ) );
      this.outputFields[ i ].setTrimType( ValueMetaBase.getTrimTypeByCode( XMLHandler.getTagValue( fnode, "trim_type" ) ) );
      this.outputFields[ i ].setNullString( XMLHandler.getTagValue( fnode, "nullif" ) );
      this.outputFields[ i ].setLength( Const.toInt( XMLHandler.getTagValue( fnode, "length" ), -1 ) );
      this.outputFields[ i ].setPrecision( Const.toInt( XMLHandler.getTagValue( fnode, "precision" ), -1 ) );
    }
  }

  @Override
  /**
   * UNABLE TO USE BaseSerializingMeta because of the complexity of the Output fields
   */
  public String getXML() {
    // Get OutputFields
    StringBuilder retval = new StringBuilder( 800 );
    retval.append( XMLHandler.addTagValue( WRITE_PAYLOAD_NODE_NAMESPACE + TAB_SELECTION_INDEX, tabSelectionIndex ) );
    retval.append( XMLHandler.addTagValue( WRITE_PAYLOAD_NODE_NAMESPACE + CONNECTION, connection ) );
    retval.append( XMLHandler.addTagValue( WRITE_PAYLOAD_NODE_NAMESPACE + VIRTUAL_FOLDERS, virtualFolders ) );
    retval.append( XMLHandler.addTagValue( WRITE_PAYLOAD_NODE_NAMESPACE + RESOURCE_NAME, resourceName ) );
    retval.append( XMLHandler.addTagValue( WRITE_PAYLOAD_NODE_NAMESPACE + FILE_FORMAT, fileFormat ) );
    retval.append( XMLHandler.addTagValue( WRITE_PAYLOAD_NODE_NAMESPACE + FILE_ALREADY_EXISTS_INDEX, fileAlreadyExistsIndex.toString() ) );
    retval.append( XMLHandler.addTagValue( WRITE_PAYLOAD_NODE_NAMESPACE + OPERATION_TYPE_INDEX, operationTypeIndex.toString() ) );
    retval.append( XMLHandler.addTagValue( WRITE_PAYLOAD_NODE_NAMESPACE + RESOURCE_ID, resourceId ) );
    retval.append( XMLHandler.addTagValue( WRITE_PAYLOAD_NODE_NAMESPACE + TAGS, tags ) );
    retval.append( XMLHandler.addTagValue( WRITE_PAYLOAD_NODE_NAMESPACE + DESCRIPTION, description ) );
    retval.append( XMLHandler.addTagValue( WRITE_PAYLOAD_NODE_NAMESPACE + PROP_A, propA ) );
    retval.append( XMLHandler.addTagValue( WRITE_PAYLOAD_NODE_NAMESPACE + PROP_B, propB ) );
    retval.append( XMLHandler.addTagValue( WRITE_PAYLOAD_NODE_NAMESPACE + PROP_C, propC ) );
    retval.append( XMLHandler.addTagValue( WRITE_PAYLOAD_NODE_NAMESPACE + PROP_D, propD ) );
    retval.append( "    <fields>" ).append( Const.CR );

    for ( int i = 0; i < this.outputFields.length; ++i ) {
      TextFileField field = this.outputFields[ i ];
      if ( field.getName() != null && field.getName().length() != 0 ) {
        retval.append( "      <field>" ).append( Const.CR );
        retval.append( FIELD_PADDING ).append( XMLHandler.addTagValue( "name", field.getName() ) );
        retval.append( FIELD_PADDING ).append( XMLHandler.addTagValue( "type", field.getTypeDesc() ) );
        retval.append( FIELD_PADDING ).append( XMLHandler.addTagValue( "format", field.getFormat() ) );
        retval.append( FIELD_PADDING ).append( XMLHandler.addTagValue( "currency", field.getCurrencySymbol() ) );
        retval.append( FIELD_PADDING ).append( XMLHandler.addTagValue( "decimal", field.getDecimalSymbol() ) );
        retval.append( FIELD_PADDING ).append( XMLHandler.addTagValue( "group", field.getGroupingSymbol() ) );
        retval.append( FIELD_PADDING ).append( XMLHandler.addTagValue( "nullif", field.getNullString() ) );
        retval.append( FIELD_PADDING ).append( XMLHandler.addTagValue( "trim_type", field.getTrimTypeCode() ) );
        retval.append( FIELD_PADDING ).append( XMLHandler.addTagValue( "length", field.getLength() ) );
        retval.append( FIELD_PADDING ).append( XMLHandler.addTagValue( "precision", field.getPrecision() ) );
        retval.append( "      </field>" ).append( Const.CR );
      }
    }

    retval.append( "    </fields>" ).append( Const.CR );
    return retval.toString();
  }

  @Override
  /**
   * UNABLE TO USE BaseSerializingMeta because of the complexity of the Output fields
   */
  @SuppressWarnings( "java:S117" )
  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
    throws KettleException {
    super.readRep( rep, metaStore, id_step, databases );
    setTabSelectionIndex( Integer.valueOf( rep.getStepAttributeString( id_step, WRITE_PAYLOAD_NODE_NAMESPACE + TAB_SELECTION_INDEX ) ) );
    setConnection( rep.getStepAttributeString( id_step, WRITE_PAYLOAD_NODE_NAMESPACE + CONNECTION ) );
    setVirtualFolders( rep.getStepAttributeString( id_step, WRITE_PAYLOAD_NODE_NAMESPACE + VIRTUAL_FOLDERS ) );
    setResourceName( rep.getStepAttributeString( id_step, WRITE_PAYLOAD_NODE_NAMESPACE + RESOURCE_NAME ) );
    setFileFormat( rep.getStepAttributeString( id_step, WRITE_PAYLOAD_NODE_NAMESPACE + FILE_FORMAT ) );
    fileAlreadyExistsIndex = Integer
      .valueOf( rep.getStepAttributeString( id_step, WRITE_PAYLOAD_NODE_NAMESPACE + FILE_ALREADY_EXISTS_INDEX ) );
    operationTypeIndex =
      Integer.valueOf( rep.getStepAttributeString( id_step, WRITE_PAYLOAD_NODE_NAMESPACE + OPERATION_TYPE_INDEX ) );
    setResourceId( rep.getStepAttributeString( id_step, WRITE_PAYLOAD_NODE_NAMESPACE + RESOURCE_ID ) );
    setDescription( rep.getStepAttributeString( id_step, WRITE_PAYLOAD_NODE_NAMESPACE + DESCRIPTION ) );
    setPropA( rep.getStepAttributeString( id_step, WRITE_PAYLOAD_NODE_NAMESPACE + PROP_A ) );
    setPropB( rep.getStepAttributeString( id_step, WRITE_PAYLOAD_NODE_NAMESPACE + PROP_B ) );
    setPropC( rep.getStepAttributeString( id_step, WRITE_PAYLOAD_NODE_NAMESPACE + PROP_C ) );
    setPropD( rep.getStepAttributeString( id_step, WRITE_PAYLOAD_NODE_NAMESPACE + PROP_D ) );

    int nrfields = rep.countNrStepAttributes( id_step, FIELD_NAME );
    this.allocate( nrfields );

    for ( int i = 0; i < nrfields; ++i ) {
      this.outputFields[ i ] = new TextFileField();
      this.outputFields[ i ].setName( rep.getStepAttributeString( id_step, i, FIELD_NAME ) );
      this.outputFields[ i ].setType( rep.getStepAttributeString( id_step, i, "field_type" ) );
      this.outputFields[ i ].setFormat( rep.getStepAttributeString( id_step, i, "field_format" ) );
      this.outputFields[ i ].setCurrencySymbol( rep.getStepAttributeString( id_step, i, "field_currency" ) );
      this.outputFields[ i ].setDecimalSymbol( rep.getStepAttributeString( id_step, i, "field_decimal" ) );
      this.outputFields[ i ].setGroupingSymbol( rep.getStepAttributeString( id_step, i, "field_group" ) );
      this.outputFields[ i ].setTrimType(
        ValueMetaBase.getTrimTypeByCode( rep.getStepAttributeString( id_step, i, "field_trim_type" ) ) );
      this.outputFields[ i ].setNullString( rep.getStepAttributeString( id_step, i, "field_nullif" ) );
      this.outputFields[ i ].setLength( (int) rep.getStepAttributeInteger( id_step, i, "field_length" ) );
      this.outputFields[ i ].setPrecision( (int) rep.getStepAttributeInteger( id_step, i, "field_precision" ) );
    }

  }

  @Override
  /**
   * UNABLE TO USE BaseSerializingMeta because of the complexity of the Output fields
   */
  @SuppressWarnings( "java:S117" )
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
    throws KettleException {
    super.saveRep( rep, metaStore, id_transformation, id_step );
    rep.saveStepAttribute( id_transformation, id_step, WRITE_PAYLOAD_NODE_NAMESPACE + TAB_SELECTION_INDEX, tabSelectionIndex );
    rep.saveStepAttribute( id_transformation, id_step, WRITE_PAYLOAD_NODE_NAMESPACE + CONNECTION, connection );
    rep.saveStepAttribute( id_transformation, id_step, WRITE_PAYLOAD_NODE_NAMESPACE + VIRTUAL_FOLDERS, virtualFolders );
    rep.saveStepAttribute( id_transformation, id_step, WRITE_PAYLOAD_NODE_NAMESPACE + RESOURCE_NAME, resourceName );
    rep.saveStepAttribute( id_transformation, id_step, WRITE_PAYLOAD_NODE_NAMESPACE + FILE_FORMAT, fileFormat );
    rep.saveStepAttribute( id_transformation, id_step, WRITE_PAYLOAD_NODE_NAMESPACE + FILE_ALREADY_EXISTS_INDEX,
      fileAlreadyExistsIndex );
    rep.saveStepAttribute( id_transformation, id_step, WRITE_PAYLOAD_NODE_NAMESPACE + OPERATION_TYPE_INDEX,
      operationTypeIndex );
    rep.saveStepAttribute( id_transformation, id_step, WRITE_PAYLOAD_NODE_NAMESPACE + RESOURCE_ID, resourceId );
    rep.saveStepAttribute( id_transformation, id_step, WRITE_PAYLOAD_NODE_NAMESPACE + TAGS, tags );
    rep.saveStepAttribute( id_transformation, id_step, WRITE_PAYLOAD_NODE_NAMESPACE + DESCRIPTION, description );
    rep.saveStepAttribute( id_transformation, id_step, WRITE_PAYLOAD_NODE_NAMESPACE + PROP_A, propA );
    rep.saveStepAttribute( id_transformation, id_step, WRITE_PAYLOAD_NODE_NAMESPACE + PROP_B, propB );
    rep.saveStepAttribute( id_transformation, id_step, WRITE_PAYLOAD_NODE_NAMESPACE + PROP_C, propC );
    rep.saveStepAttribute( id_transformation, id_step, WRITE_PAYLOAD_NODE_NAMESPACE + PROP_D, propD );

    for ( int i = 0; i < this.outputFields.length; ++i ) {
      TextFileField field = this.outputFields[ i ];
      rep.saveStepAttribute( id_transformation, id_step, i, FIELD_NAME, field.getName() );
      rep.saveStepAttribute( id_transformation, id_step, i, "field_type", field.getTypeDesc() );
      rep.saveStepAttribute( id_transformation, id_step, i, "field_format", field.getFormat() );
      rep.saveStepAttribute( id_transformation, id_step, i, "field_currency", field.getCurrencySymbol() );
      rep.saveStepAttribute( id_transformation, id_step, i, "field_decimal", field.getDecimalSymbol() );
      rep.saveStepAttribute( id_transformation, id_step, i, "field_group", field.getGroupingSymbol() );
      rep.saveStepAttribute( id_transformation, id_step, i, "field_trim_type", field.getTrimTypeCode() );
      rep.saveStepAttribute( id_transformation, id_step, i, "field_nullif", field.getNullString() );
      rep.saveStepAttribute( id_transformation, id_step, i, "field_length", (long) field.getLength() );
      rep.saveStepAttribute( id_transformation, id_step, i, "field_precision", (long) field.getPrecision() );
    }
  }

  public void setTabSelectionIndex( Integer tabSelectionIndex ) {
    this.tabSelectionIndex = tabSelectionIndex;
  }

}
