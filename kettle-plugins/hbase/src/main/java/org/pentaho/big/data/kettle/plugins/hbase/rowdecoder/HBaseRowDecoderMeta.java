/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2023 by Hitachi Vantara : http://www.pentaho.com
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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.big.data.kettle.plugins.hbase.MappingDefinition;
import org.pentaho.big.data.kettle.plugins.hbase.NamedClusterLoadSaveUtil;
import org.pentaho.big.data.kettle.plugins.hbase.mapping.MappingUtils;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionDeep;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.core.service.PluginServiceLoader;
import org.pentaho.di.core.variables.VariableSpace;
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
import org.pentaho.hadoop.shim.api.cluster.ClusterInitializationException;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.hadoop.shim.api.hbase.HBaseService;
import org.pentaho.hadoop.shim.api.hbase.mapping.Mapping;
import org.pentaho.hadoop.shim.api.hbase.meta.HBaseValueMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.locator.api.MetastoreLocator;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;
import org.w3c.dom.Node;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.pentaho.di.core.CheckResult.TYPE_RESULT_ERROR;
import static org.pentaho.di.core.CheckResult.TYPE_RESULT_OK;
import static org.pentaho.di.core.CheckResult.TYPE_RESULT_WARNING;

/**
 * Meta class for the HBase row decoder.
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 *
 */
@Step( id = "HBaseRowDecoder", image = "HBRD.svg", name = "HBaseRowDecoder.Name",
    description = "HBaseRowDecoder.Description",
    categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.BigData",
    documentationUrl = "Products/HBase_Row_Decoder",
    i18nPackageName = "org.pentaho.di.trans.steps.hbaserowdecoder" )
@InjectionSupported( localizationPrefix = "HBaseRowDecoder.Injection.", groups = { "MAPPING" } )
public class HBaseRowDecoderMeta extends BaseStepMeta implements StepMetaInterface {

  public static final String INCOMING_KEY_FIELD = "incoming_key_field";
  public static final String INCOMING_RESULT_FIELD = "incoming_result_field";
  protected NamedCluster namedCluster;

  /** The incoming field that contains the HBase row key */
  @Injection( name = "KEY_FIELD" )
  protected String mIncomingKeyField = "";

  /** The incoming field that contains the HBase row Result object */
  @Injection( name = "HBASE_RESULT_FIELD" )
  protected String mIncomingResultField = "";

  /** The mapping to use */
  protected Mapping mMapping;

  @InjectionDeep
  protected MappingDefinition mappingDefinition;

  private MetastoreLocator metaStoreService;
  private final NamedClusterServiceLocator namedClusterServiceLocator;
  private final NamedClusterService namedClusterService;
  private final RuntimeTestActionService runtimeTestActionService;
  private final RuntimeTester runtimeTester;

  private final NamedClusterLoadSaveUtil namedClusterLoadSaveUtil;

  public HBaseRowDecoderMeta( NamedClusterServiceLocator namedClusterServiceLocator,
                              NamedClusterService namedClusterService,
                              RuntimeTestActionService runtimeTestActionService, RuntimeTester runtimeTester ) {
    this( namedClusterServiceLocator, namedClusterService, runtimeTestActionService, runtimeTester, null );
  }

  public MetastoreLocator getMetastoreLocators() {
    if ( this.metaStoreService == null ) {
      try {
        Collection<MetastoreLocator> metastoreLocators = PluginServiceLoader.loadServices( MetastoreLocator.class );
        this.metaStoreService = metastoreLocators.stream().findFirst().get();
      } catch ( Exception e ) {
        logError( "Error getting MetastoreLocator", e );
      }
    }
    return this.metaStoreService;
  }

  @VisibleForTesting
  HBaseRowDecoderMeta( NamedClusterServiceLocator namedClusterServiceLocator,
                              NamedClusterService namedClusterService,
                              RuntimeTestActionService runtimeTestActionService, RuntimeTester runtimeTester, MetastoreLocator metaStore ) {
    this.namedClusterServiceLocator = namedClusterServiceLocator;
    this.namedClusterService = namedClusterService;
    this.runtimeTestActionService = runtimeTestActionService;
    this.runtimeTester = runtimeTester;
    this.namedClusterLoadSaveUtil = new NamedClusterLoadSaveUtil();
    this.metaStoreService = metaStore;
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
    mIncomingKeyField = inKey;
  }

  /**
   * Get the incoming field that holds the HBase row key
   *
   * @return the name of the field that holds the key
   */
  public String getIncomingKeyField() {
    return mIncomingKeyField;
  }

  /**
   * Set the incoming field that holds the HBase row Result object
   *
   * @param inResult
   *          the name of the field that holds the HBase row Result object
   */
  public void setIncomingResultField( String inResult ) {
    mIncomingResultField = inResult;
  }

  /**
   * Get the incoming field that holds the HBase row Result object
   *
   * @return the name of the field that holds the HBase row Result object
   */
  public String getIncomingResultField() {
    return mIncomingResultField;
  }

  /**
   * Set the mapping to use for decoding the row
   *
   * @param m
   *          the mapping to use
   */
  public void setMapping( Mapping m ) {
    mMapping = m;
  }

  /**
   * Get the mapping to use for decoding the row
   *
   * @return the mapping to use
   */
  public Mapping getMapping() {
    return mMapping;
  }

  public MappingDefinition getMappingDefinition() {
    return mappingDefinition;
  }

  public void setMappingDefinition( MappingDefinition mappingDefinition ) {
    this.mappingDefinition = mappingDefinition;
  }

  public void setDefault() {
    mIncomingKeyField = "";
    mIncomingResultField = "";
    namedCluster = namedClusterService.getClusterTemplate();
  }

  @Override
  public void getFields( RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
      VariableSpace space ) throws KettleStepException {

    rowMeta.clear(); // start afresh - eats the input

    if ( mMapping != null ) {
      int kettleType;

      if ( mMapping.getKeyType() == Mapping.KeyType.DATE
        || mMapping.getKeyType() == Mapping.KeyType.UNSIGNED_DATE ) {
        kettleType = ValueMetaInterface.TYPE_DATE;
      } else if ( mMapping.getKeyType() == Mapping.KeyType.STRING ) {
        kettleType = ValueMetaInterface.TYPE_STRING;
      } else if ( mMapping.getKeyType() == Mapping.KeyType.BINARY ) {
        kettleType = ValueMetaInterface.TYPE_BINARY;
      } else {
        kettleType = ValueMetaInterface.TYPE_INTEGER;
      }

      ValueMetaInterface keyMeta = new ValueMetaBase( mMapping.getKeyName(), kettleType );

      keyMeta.setOrigin( origin );
      rowMeta.addValueMeta( keyMeta );

      // Add the rest of the fields in the mapping
      Map<String, HBaseValueMetaInterface> mappedColumnsByAlias = mMapping.getMappedColumns();
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
          TYPE_RESULT_WARNING, "Not receiving any fields from previous steps!", stepMeta );
      remarks.add( cr );
    } else {
      cr =
          new CheckResult( TYPE_RESULT_OK, "Step is connected to previous one, receiving " + prev.size()
              + " fields", stepMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr = new CheckResult( TYPE_RESULT_OK, "Step is receiving info from other steps.", stepMeta );
      remarks.add( cr );
    } else {
      cr = new CheckResult( TYPE_RESULT_ERROR, "No input received from other steps!", stepMeta );
      remarks.add( cr );
    }
  }

  void applyInjection() throws KettleException {
    if ( namedCluster == null ) {
      throw new KettleException( "Named cluster was not initialized!" );
    }
    try {
      HBaseService hBaseService = namedClusterServiceLocator.getService( this.namedCluster, HBaseService.class );
      Mapping tempMapping = null;
      if ( mappingDefinition != null ) {
        tempMapping = MappingUtils.getMapping( mappingDefinition, hBaseService );
        mMapping = tempMapping;
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
      applyInjection();
    } catch ( KettleException e ) {
      log.logError( "Error occurred while injecting metadata. Transformation meta could be incorrect!", e );
    }
    StringBuilder retval = new StringBuilder();

    if ( StringUtils.isNotEmpty( mIncomingKeyField ) ) {
      retval.append( "\n    " ).append( XMLHandler.addTagValue( INCOMING_KEY_FIELD, mIncomingKeyField ) );
    }
    if ( StringUtils.isNotEmpty( mIncomingResultField ) ) {
      retval.append( "\n    " ).append( XMLHandler.addTagValue( INCOMING_RESULT_FIELD, mIncomingResultField ) );
    }

    namedClusterLoadSaveUtil.getXml( retval, namedClusterService, namedCluster, repository == null ? null : repository
        .getMetaStore(), log );
    if ( mMapping != null ) {
      retval.append( mMapping.getXML() );
    }

    return retval.toString();
  }

  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    if ( metaStore == null ) {
      metaStore = getMetastoreLocators().getMetastore();
    }

    mIncomingKeyField = XMLHandler.getTagValue( stepnode, INCOMING_KEY_FIELD );
    mIncomingResultField = XMLHandler.getTagValue( stepnode, INCOMING_RESULT_FIELD );
    this.namedCluster =
        namedClusterLoadSaveUtil.loadClusterConfig( namedClusterService, null, repository, metaStore, stepnode, log );
    try {
      HBaseService hbaseService = namedClusterServiceLocator.getService( this.namedCluster, HBaseService.class );
      mMapping = ( hbaseService == null ? null : hbaseService.getMappingFactory().createMapping() );
    } catch ( ClusterInitializationException e ) {
      throw new KettleXMLException( e );
    }
    if ( mMapping != null ) {
      mMapping.loadXML( stepnode );
    }
  }

  public void readRep( Repository rep, IMetaStore metaStore, ObjectId idStep, List<DatabaseMeta> databases )
    throws KettleException {

    mIncomingKeyField = rep.getStepAttributeString( idStep, 0, INCOMING_KEY_FIELD );
    mIncomingResultField = rep.getStepAttributeString( idStep, 0, INCOMING_RESULT_FIELD );
    this.namedCluster =
        namedClusterLoadSaveUtil.loadClusterConfig( namedClusterService, idStep, rep, metaStore, null, log );
    try {
      mMapping =
          namedClusterServiceLocator.getService( this.namedCluster, HBaseService.class ).getMappingFactory()
              .createMapping();
    } catch ( ClusterInitializationException e ) {
      throw new KettleXMLException( e );
    }
    mMapping.readRep( rep, idStep );
  }

  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId idTransformation, ObjectId idStep ) throws KettleException {

    if ( StringUtils.isNotEmpty( mIncomingKeyField ) ) {
      rep.saveStepAttribute( idTransformation, idStep, 0, INCOMING_KEY_FIELD, mIncomingKeyField );
    }
    if ( StringUtils.isNotEmpty( mIncomingResultField ) ) {
      rep.saveStepAttribute( idTransformation, idStep, 0, INCOMING_RESULT_FIELD, mIncomingResultField );
    }

    namedClusterLoadSaveUtil.saveRep( rep, metaStore, idTransformation, idStep, namedClusterService, namedCluster, log );

    if ( mMapping != null ) {
      mMapping.saveRep( rep, idTransformation, idStep );
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
