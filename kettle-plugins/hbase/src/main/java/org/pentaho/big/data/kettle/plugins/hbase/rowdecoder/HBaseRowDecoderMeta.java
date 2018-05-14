/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.hbase.rowdecoder;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.hadoop.shim.api.cluster.ClusterInitializationException;
import org.pentaho.big.data.kettle.plugins.hbase.MappingDefinition;
import org.pentaho.big.data.kettle.plugins.hbase.NamedClusterLoadSaveUtil;
import org.pentaho.big.data.kettle.plugins.hbase.mapping.MappingUtils;
import org.pentaho.hadoop.shim.api.hbase.HBaseService;
import org.pentaho.hadoop.shim.api.hbase.mapping.Mapping;
import org.pentaho.hadoop.shim.api.hbase.meta.HBaseValueMetaInterface;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionDeep;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;
import org.w3c.dom.Node;

/**
 * Meta class for the HBase row decoder.
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 *
 */
@Step( id = "HBaseRowDecoder", image = "HBRD.svg", name = "HBaseRowDecoder.Name",
    description = "HBaseRowDecoder.Description",
    categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.BigData",
    documentationUrl = "http://wiki.pentaho.com/display/EAI/HBase+Row+Decoder",
    i18nPackageName = "org.pentaho.di.trans.steps.hbaserowdecoder" )
@InjectionSupported( localizationPrefix = "HBaseRowDecoder.Injection.", groups = { "MAPPING" } )
public class HBaseRowDecoderMeta extends BaseStepMeta implements StepMetaInterface {

  protected NamedCluster namedCluster;

  /** The incoming field that contains the HBase row key */
  @Injection( name = "KEY_FIELD" )
  protected String m_incomingKeyField = "";

  /** The incoming field that contains the HBase row Result object */
  @Injection( name = "HBASE_RESULT_FIELD" )
  protected String m_incomingResultField = "";

  /** The mapping to use */
  protected Mapping m_mapping;

  @InjectionDeep
  protected MappingDefinition mappingDefinition;

  private final NamedClusterServiceLocator namedClusterServiceLocator;
  private final NamedClusterService namedClusterService;
  private final RuntimeTestActionService runtimeTestActionService;
  private final RuntimeTester runtimeTester;

  private final NamedClusterLoadSaveUtil namedClusterLoadSaveUtil;

  public HBaseRowDecoderMeta( NamedClusterServiceLocator namedClusterServiceLocator,
                              NamedClusterService namedClusterService,
                              RuntimeTestActionService runtimeTestActionService, RuntimeTester runtimeTester ) {
    this.namedClusterServiceLocator = namedClusterServiceLocator;
    this.namedClusterService = namedClusterService;
    this.runtimeTestActionService = runtimeTestActionService;
    this.runtimeTester = runtimeTester;
    this.namedClusterLoadSaveUtil = new NamedClusterLoadSaveUtil();
  }



  /**
   * @param namedCluster the namedCluster to set
   */
  public void setNamedCluster( NamedCluster namedCluster ) {
    this.namedCluster = namedCluster;
  }

  /**
   * @return the namedCluster
   */
  public NamedCluster getNamedCluster() {
    return namedCluster;
  }


  /**
   * Set the incoming field that holds the HBase row key
   *
   * @param inKey
   *          the name of the field that holds the key
   */
  public void setIncomingKeyField( String inKey ) {
    m_incomingKeyField = inKey;
  }

  /**
   * Get the incoming field that holds the HBase row key
   *
   * @return the name of the field that holds the key
   */
  public String getIncomingKeyField() {
    return m_incomingKeyField;
  }

  /**
   * Set the incoming field that holds the HBase row Result object
   *
   * @param inResult
   *          the name of the field that holds the HBase row Result object
   */
  public void setIncomingResultField( String inResult ) {
    m_incomingResultField = inResult;
  }

  /**
   * Get the incoming field that holds the HBase row Result object
   *
   * @return the name of the field that holds the HBase row Result object
   */
  public String getIncomingResultField() {
    return m_incomingResultField;
  }

  /**
   * Set the mapping to use for decoding the row
   *
   * @param m
   *          the mapping to use
   */
  public void setMapping( Mapping m ) {
    m_mapping = m;
  }

  /**
   * Get the mapping to use for decoding the row
   *
   * @return the mapping to use
   */
  public Mapping getMapping() {
    return m_mapping;
  }

  public MappingDefinition getMappingDefinition() {
    return mappingDefinition;
  }

  public void setMappingDefinition( MappingDefinition mappingDefinition ) {
    this.mappingDefinition = mappingDefinition;
  }

  public void setDefault() {
    m_incomingKeyField = "";
    m_incomingResultField = "";
    namedCluster = namedClusterService.getClusterTemplate();
  }

  @Override
  public void getFields( RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
      VariableSpace space ) throws KettleStepException {

    rowMeta.clear(); // start afresh - eats the input

    if ( m_mapping != null ) {
      int kettleType;

      if ( m_mapping.getKeyType() == Mapping.KeyType.DATE
        || m_mapping.getKeyType() == Mapping.KeyType.UNSIGNED_DATE ) {
        kettleType = ValueMetaInterface.TYPE_DATE;
      } else if ( m_mapping.getKeyType() == Mapping.KeyType.STRING ) {
        kettleType = ValueMetaInterface.TYPE_STRING;
      } else if ( m_mapping.getKeyType() == Mapping.KeyType.BINARY ) {
        kettleType = ValueMetaInterface.TYPE_BINARY;
      } else {
        kettleType = ValueMetaInterface.TYPE_INTEGER;
      }

      ValueMetaInterface keyMeta = new ValueMeta( m_mapping.getKeyName(), kettleType );

      keyMeta.setOrigin( origin );
      rowMeta.addValueMeta( keyMeta );

      // Add the rest of the fields in the mapping
      Map<String, HBaseValueMetaInterface> mappedColumnsByAlias = m_mapping.getMappedColumns();
      Set<String> aliasSet = mappedColumnsByAlias.keySet();
      for ( String alias : aliasSet ) {
        HBaseValueMetaInterface columnMeta = mappedColumnsByAlias.get( alias );
        columnMeta.setOrigin( origin );
        rowMeta.addValueMeta( columnMeta );
      }
    }
  }

  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
      String[] input, String[] output, RowMetaInterface info ) {

    CheckResult cr;

    if ( ( prev == null ) || ( prev.size() == 0 ) ) {
      cr = new CheckResult(
          CheckResult.TYPE_RESULT_WARNING, "Not receiving any fields from previous steps!", stepMeta );
      remarks.add( cr );
    } else {
      cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, "Step is connected to previous one, receiving " + prev.size()
              + " fields", stepMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, "Step is receiving info from other steps.", stepMeta );
      remarks.add( cr );
    } else {
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, "No input received from other steps!", stepMeta );
      remarks.add( cr );
    }
  }

  void applyInjection( VariableSpace space ) throws KettleException {
    if ( namedCluster == null ) {
      throw new KettleException( "Named cluster was not initialized!" );
    }
    try {
      HBaseService hBaseService = namedClusterServiceLocator.getService( this.namedCluster, HBaseService.class );
      Mapping tempMapping = null;
      if ( mappingDefinition != null ) {
        tempMapping = MappingUtils.getMapping( mappingDefinition, hBaseService );
        m_mapping = tempMapping;
      }
    } catch ( ClusterInitializationException e ) {
      throw new KettleException( e );
    }
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
      TransMeta transMeta, Trans trans ) {

    return new HBaseRowDecoder( stepMeta, stepDataInterface, copyNr, transMeta, trans, namedClusterServiceLocator );
  }

  public StepDataInterface getStepData() {
    return new HBaseRowDecoderData();
  }

  @Override
  public String getXML() {
    try {
      applyInjection( new Variables() );
    } catch ( KettleException e ) {
      log.logError( "Error occurred while injecting metadata. Transformation meta could be incorrect!", e );
    }
    StringBuilder retval = new StringBuilder();

    if ( !Const.isEmpty( m_incomingKeyField ) ) {
      retval.append( "\n    " ).append( XMLHandler.addTagValue( "incoming_key_field", m_incomingKeyField ) );
    }
    if ( !Const.isEmpty( m_incomingResultField ) ) {
      retval.append( "\n    " ).append( XMLHandler.addTagValue( "incoming_result_field", m_incomingResultField ) );
    }

    namedClusterLoadSaveUtil.getXml( retval, namedClusterService, namedCluster, repository == null ? null : repository
        .getMetaStore(), log );
    if ( m_mapping != null ) {
      retval.append( m_mapping.getXML() );
    }

    return retval.toString();
  }

  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    m_incomingKeyField = XMLHandler.getTagValue( stepnode, "incoming_key_field" );
    m_incomingResultField = XMLHandler.getTagValue( stepnode, "incoming_result_field" );
    this.namedCluster =
        namedClusterLoadSaveUtil.loadClusterConfig( namedClusterService, null, repository, metaStore, stepnode, log );
    try {
      m_mapping =
          namedClusterServiceLocator.getService( this.namedCluster, HBaseService.class ).getMappingFactory()
              .createMapping();
    } catch ( ClusterInitializationException e ) {
      throw new KettleXMLException( e );
    }
    m_mapping.loadXML( stepnode );
  }

  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
    throws KettleException {

    m_incomingKeyField = rep.getStepAttributeString( id_step, 0, "incoming_key_field" );
    m_incomingResultField = rep.getStepAttributeString( id_step, 0, "incoming_result_field" );
    this.namedCluster =
        namedClusterLoadSaveUtil.loadClusterConfig( namedClusterService, id_step, rep, metaStore, null, log );
    try {
      m_mapping =
          namedClusterServiceLocator.getService( this.namedCluster, HBaseService.class ).getMappingFactory()
              .createMapping();
    } catch ( ClusterInitializationException e ) {
      throw new KettleXMLException( e );
    }
    m_mapping.readRep( rep, id_step );
  }

  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step ) throws KettleException {

    if ( !Const.isEmpty( m_incomingKeyField ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "incoming_key_field", m_incomingKeyField );
    }
    if ( !Const.isEmpty( m_incomingResultField ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "incoming_result_field", m_incomingResultField );
    }

    namedClusterLoadSaveUtil.saveRep( rep, metaStore, id_transformation, id_step, namedClusterService, namedCluster, log );

    if ( m_mapping != null ) {
      m_mapping.saveRep( rep, id_transformation, id_step );
    }
  }

  /**
   * Get the UI for this step.
   *
   * @param shell
   *          a <code>Shell</code> value
   * @param meta
   *          a <code>StepMetaInterface</code> value
   * @param transMeta
   *          a <code>TransMeta</code> value
   * @param name
   *          a <code>String</code> value
   * @return a <code>StepDialogInterface</code> value
   */
  public StepDialogInterface getDialog( Shell shell, StepMetaInterface meta, TransMeta transMeta, String name ) {
    return new HBaseRowDecoderDialog( shell, meta, transMeta, name, namedClusterService, runtimeTestActionService,
      runtimeTester, namedClusterServiceLocator );
  }

}
