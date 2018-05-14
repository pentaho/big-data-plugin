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

package org.pentaho.big.data.kettle.plugins.hbase.output;

import java.util.List;

import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.hadoop.shim.api.cluster.ClusterInitializationException;
import org.pentaho.big.data.kettle.plugins.hbase.MappingDefinition;
import org.pentaho.big.data.kettle.plugins.hbase.NamedClusterLoadSaveUtil;
import org.pentaho.big.data.kettle.plugins.hbase.ServiceStatus;
import org.pentaho.big.data.kettle.plugins.hbase.mapping.MappingUtils;
import org.pentaho.big.data.kettle.plugins.hbase.meta.AELHBaseMappingImpl;
import org.pentaho.hadoop.shim.api.hbase.HBaseService;
import org.pentaho.hadoop.shim.api.hbase.mapping.Mapping;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionDeep;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.osgi.api.MetastoreLocatorOsgi;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;
import org.w3c.dom.Node;

import com.google.common.annotations.VisibleForTesting;

/**
 * Class providing an output step for writing data to an HBase table according to meta data column/type mapping info
 * stored in a separate HBase table called "pentaho_mappings". See org.pentaho.hbase.mapping.Mapping for details on the
 * meta data format.
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
@Step( id = "HBaseOutput", image = "HBO.svg", name = "HBaseOutput.Name", description = "HBaseOutput.Description",
  categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.BigData",
  documentationUrl = "Products/HBase_Output",
  i18nPackageName = "org.pentaho.di.trans.steps.hbaseoutput" )
@InjectionSupported( localizationPrefix = "HBaseOutput.Injection.", groups = { "MAPPING" } )
public class HBaseOutputMeta extends BaseStepMeta implements StepMetaInterface {

  protected static Class<?> PKG = HBaseOutputMeta.class;

  /**
   * path/url to hbase-site.xml
   */
  @Injection( name = "HBASE_SITE_XML_URL" )
  protected String m_coreConfigURL;

  /**
   * path/url to hbase-default.xml
   */
  @Injection( name = "HBASE_DEFAULT_XML_URL" )
  protected String m_defaultConfigURL;

  /**
   * the name of the HBase table to write to
   */
  @Injection( name = "TARGET_TABLE_NAME" )
  protected String m_targetTableName;

  /**
   * the name of the mapping for columns/types for the target table
   */
  @Injection( name = "TARGET_MAPPING_NAME" )
  protected String m_targetMappingName;

  /**
   * if true then the incoming column with row key from the mapping will be deleted
   */
  @Injection( name = "DELETE_ROW_KEY" )
  protected boolean m_deleteRowKey;

  /**
   * if true then the WAL will not be written to
   */
  @Injection( name = "DISABLE_WRITE_TO_WAL" )
  protected boolean m_disableWriteToWAL;

  /**
   * The size of the write buffer in bytes (empty - default from hbase-default.xml is used)
   */
  @Injection( name = "WRITE_BUFFER_SIZE" )
  protected String m_writeBufferSize;

  /**
   * The mapping to use if we are not loading one dynamically at runtime from HBase itself
   */
  protected Mapping m_mapping;

  @InjectionDeep
  protected MappingDefinition mappingDefinition;

  private NamedCluster namedCluster;

  private final NamedClusterLoadSaveUtil namedClusterLoadSaveUtil;
  private final NamedClusterService namedClusterService;
  private final NamedClusterServiceLocator namedClusterServiceLocator;
  private final RuntimeTestActionService runtimeTestActionService;
  private final RuntimeTester runtimeTester;
  private MetastoreLocatorOsgi metaStoreService;
  private ServiceStatus serviceStatus = ServiceStatus.OK;

  public NamedClusterService getNamedClusterService() {
    return namedClusterService;
  }

  public NamedClusterServiceLocator getNamedClusterServiceLocator() {
    return namedClusterServiceLocator;
  }

  public RuntimeTestActionService getRuntimeTestActionService() {
    return runtimeTestActionService;
  }

  public RuntimeTester getRuntimeTester() {
    return runtimeTester;
  }

  public HBaseOutputMeta( NamedClusterService namedClusterService,
                          NamedClusterServiceLocator namedClusterServiceLocator,
                          RuntimeTestActionService runtimeTestActionService, RuntimeTester runtimeTester,
                          MetastoreLocatorOsgi metaStore ) {
    this( namedClusterService, namedClusterServiceLocator,
      runtimeTestActionService, runtimeTester, new NamedClusterLoadSaveUtil(), metaStore );
  }

  @VisibleForTesting
  protected HBaseOutputMeta( NamedClusterService namedClusterService,
                             NamedClusterServiceLocator namedClusterServiceLocator,
                             RuntimeTestActionService runtimeTestActionService, RuntimeTester runtimeTester,
                             NamedClusterLoadSaveUtil namedClusterLoadSaveUtil, MetastoreLocatorOsgi metaStore ) {
    this.namedClusterService = namedClusterService;
    this.namedClusterServiceLocator = namedClusterServiceLocator;
    this.runtimeTestActionService = runtimeTestActionService;

    this.runtimeTester = runtimeTester;
    this.namedClusterLoadSaveUtil = namedClusterLoadSaveUtil;
    this.metaStoreService = metaStore;
  }

  /**
   * Set the mapping to use for decoding the row
   *
   * @param m the mapping to use
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

  public void setCoreConfigURL( String coreConfig ) {
    m_coreConfigURL = coreConfig;
  }

  public String getCoreConfigURL() {
    return m_coreConfigURL;
  }

  public void setDefaulConfigURL( String defaultConfig ) {
    m_defaultConfigURL = defaultConfig;
  }

  public String getDefaultConfigURL() {
    return m_defaultConfigURL;
  }

  public void setTargetTableName( String targetTable ) {
    m_targetTableName = targetTable;
  }

  public String getTargetTableName() {
    return m_targetTableName;
  }

  public void setTargetMappingName( String targetMapping ) {
    m_targetMappingName = targetMapping;
  }

  public String getTargetMappingName() {
    return m_targetMappingName;
  }

  public boolean getDeleteRowKey() {
    return m_deleteRowKey;
  }

  public void setDeleteRowKey( boolean m_deleteRowKey ) {
    this.m_deleteRowKey = m_deleteRowKey;
  }

  public void setDisableWriteToWAL( boolean d ) {
    m_disableWriteToWAL = d;
  }

  public boolean getDisableWriteToWAL() {
    return m_disableWriteToWAL;
  }

  public void setWriteBufferSize( String size ) {
    m_writeBufferSize = size;
  }

  public String getWriteBufferSize() {
    return m_writeBufferSize;
  }

  void applyInjection( VariableSpace space ) throws KettleException {
    if ( namedCluster == null ) {
      throw new KettleException( "Named cluster was not initialized!" );
    }
    try {
      if ( mappingDefinition == null ) {
        ServiceStatus serviceStatus = this.getServiceStatus();
        if ( !serviceStatus.isOk() ) {
          throw serviceStatus.getException();
        }
        return;
      }
      HBaseService hBaseService = getService();
      Mapping tempMapping = null;
      tempMapping = getMapping( mappingDefinition, hBaseService );
      setMapping( tempMapping );
    } catch ( Exception e ) {
      throw new KettleException( e );
    }
  }

  @VisibleForTesting
  Mapping getMapping( MappingDefinition mappingDefinition, HBaseService hBaseService ) throws KettleException {
    return MappingUtils.getMapping( mappingDefinition, hBaseService );
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

  @Override
  public String getXML() {
    try {
      applyInjection( new Variables() );
    } catch ( KettleException e ) {
      logError( "Error occurred while injecting metadata. Transformation meta could be incorrect!", e );
    }
    StringBuilder retval = new StringBuilder();
    namedClusterLoadSaveUtil
      .getXml( retval, namedClusterService, namedCluster, repository == null ? null : repository.getMetaStore(),
        getLog() );

    if ( !Utils.isEmpty( m_coreConfigURL ) ) {
      retval.append( "\n    " ).append( XMLHandler.addTagValue( "core_config_url", m_coreConfigURL ) );
    }
    if ( !Utils.isEmpty( m_defaultConfigURL ) ) {
      retval.append( "\n    " ).append( XMLHandler.addTagValue( "default_config_url", m_defaultConfigURL ) );
    }
    if ( !Utils.isEmpty( m_targetTableName ) ) {
      retval.append( "\n    " ).append( XMLHandler.addTagValue( "target_table_name", m_targetTableName ) );
    }
    if ( !Utils.isEmpty( m_targetMappingName ) ) {
      retval.append( "\n    " ).append( XMLHandler.addTagValue( "target_mapping_name", m_targetMappingName ) );
    }

    retval.append( "\n    " ).append( XMLHandler.addTagValue( "delete_rows_by_key", m_deleteRowKey ) );

    if ( !Utils.isEmpty( m_writeBufferSize ) ) {
      retval.append( "\n    " ).append( XMLHandler.addTagValue( "write_buffer_size", m_writeBufferSize ) );
    }
    retval.append( "\n    " ).append( XMLHandler.addTagValue( "disable_wal", m_disableWriteToWAL ) );


    if ( m_mapping != null ) {
      retval.append( m_mapping.getXML() );
    }

    return retval.toString();
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
                                TransMeta transMeta, Trans trans ) {
    return new HBaseOutput( stepMeta, stepDataInterface, copyNr, transMeta, trans, namedClusterServiceLocator );
  }

  public StepDataInterface getStepData() {
    return new HBaseOutputData();
  }

  @Override public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore )
    throws KettleXMLException {

    if ( metaStore == null ) {
      metaStore = metaStoreService.getMetastore();
    }

    this.namedCluster =
      namedClusterLoadSaveUtil.loadClusterConfig( namedClusterService, null, null, metaStore, stepnode, getLog() );

    m_coreConfigURL = XMLHandler.getTagValue( stepnode, "core_config_url" );
    m_defaultConfigURL = XMLHandler.getTagValue( stepnode, "default_config_url" );
    m_targetTableName = XMLHandler.getTagValue( stepnode, "target_table_name" );
    m_targetMappingName = XMLHandler.getTagValue( stepnode, "target_mapping_name" );
    String deleteKeys = XMLHandler.getTagValue( stepnode, "delete_rows_by_key" );
    if ( !Utils.isEmpty( deleteKeys ) ) {
      m_deleteRowKey = deleteKeys.equalsIgnoreCase( "Y" );
    }
    m_writeBufferSize = XMLHandler.getTagValue( stepnode, "write_buffer_size" );
    String disableWAL = XMLHandler.getTagValue( stepnode, "disable_wal" );
    m_disableWriteToWAL = disableWAL.equalsIgnoreCase( "Y" );

    Mapping tempMapping = null;
    try {
      tempMapping =
        getService().getMappingFactory().createMapping();
    } catch ( Exception e ) {
      getLog().logError( e.getMessage() );
    }

    /**
     * Assume that null mappings indicate
     * a missing HBaseService.  Try loading
     * from KTR
     */
    if ( tempMapping == null ) {
      tempMapping = new AELHBaseMappingImpl();
    }

    if ( tempMapping != null && tempMapping.loadXML( stepnode ) ) {
      m_mapping = tempMapping;
    } else {
      m_mapping = null;
    }
  }

  @Override public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
    throws KettleException {

    if ( metaStore == null ) {
      metaStore = metaStoreService.getMetastore();
    }

    this.namedCluster =
      namedClusterLoadSaveUtil.loadClusterConfig( namedClusterService, id_step, rep, metaStore, null, getLog() );
    m_coreConfigURL = rep.getStepAttributeString( id_step, 0, "core_config_url" );
    m_defaultConfigURL = rep.getStepAttributeString( id_step, 0, "default_config_url" );
    m_targetTableName = rep.getStepAttributeString( id_step, 0, "target_table_name" );
    m_targetMappingName = rep.getStepAttributeString( id_step, 0, "target_mapping_name" );
    m_deleteRowKey = rep.getStepAttributeBoolean( id_step, 0, "delete_rows_by_key" );
    m_writeBufferSize = rep.getStepAttributeString( id_step, 0, "write_buffer_size" );
    m_disableWriteToWAL = rep.getStepAttributeBoolean( id_step, 0, "disable_wal" );

    Mapping tempMapping = null;
    try {
      tempMapping =
        getService().getMappingFactory().createMapping();
    } catch ( Exception e ) {
      getLog().logError( e.getMessage() );
    }
    if ( tempMapping != null && tempMapping.readRep( rep, id_step ) ) {
      m_mapping = tempMapping;
    } else {
      m_mapping = null;
    }
  }

  @Override public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
    throws KettleException {

    if ( metaStore == null ) {
      metaStore = metaStoreService.getMetastore();
    }

    namedClusterLoadSaveUtil
      .saveRep( rep, metaStore, id_transformation, id_step, namedClusterService, namedCluster, getLog() );

    if ( !Utils.isEmpty( m_coreConfigURL ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "core_config_url", m_coreConfigURL );
    }
    if ( !Utils.isEmpty( m_defaultConfigURL ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "default_config_url", m_defaultConfigURL );
    }
    if ( !Utils.isEmpty( m_targetTableName ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "target_table_name", m_targetTableName );
    }
    if ( !Utils.isEmpty( m_targetMappingName ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "target_mapping_name", m_targetMappingName );
    }

    rep.saveStepAttribute( id_transformation, id_step, 0, "delete_rows_by_key", m_deleteRowKey );

    if ( !Utils.isEmpty( m_writeBufferSize ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "write_buffer_size", m_writeBufferSize );
    }
    rep.saveStepAttribute( id_transformation, id_step, 0, "disable_wal", m_disableWriteToWAL );

    if ( m_mapping != null ) {
      m_mapping.saveRep( rep, id_transformation, id_step );
    }
  }

  public void setDefault() {
    m_coreConfigURL = null;
    m_defaultConfigURL = null;
    m_targetTableName = null;
    m_targetMappingName = null;
    m_deleteRowKey = false;
    m_disableWriteToWAL = false;
    m_writeBufferSize = null;
    namedCluster = namedClusterService.getClusterTemplate();
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  public NamedCluster getNamedCluster() {
    return namedCluster;
  }

  public void setNamedCluster( NamedCluster namedCluster ) {
    this.namedCluster = namedCluster;
  }

  public MappingDefinition getMappingDefinition() {
    return mappingDefinition;
  }

  public void setMappingDefinition( MappingDefinition mappingDefinition ) {
    this.mappingDefinition = mappingDefinition;
  }

  protected HBaseService getService() throws ClusterInitializationException {
    HBaseService service = null;
    try {
      service = namedClusterServiceLocator.getService( this.namedCluster, HBaseService.class );
      this.serviceStatus = ServiceStatus.OK;
    } catch ( Exception e ) {
      this.serviceStatus = ServiceStatus.notOk( e );
      logError( Messages.getString( "HBaseOutput.Error.ServiceStatus" ) );
      throw e;
    }
    return service;
  }

  public ServiceStatus getServiceStatus() {
    if ( this.serviceStatus == null ) {
      this.serviceStatus = ServiceStatus.OK;
    }
    return this.serviceStatus;
  }
}
