/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.big.data.kettle.plugins.hdfs.trans;

import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.bowl.Bowl;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.metastore.MetaStoreConst;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.resource.ResourceNamingInterface;
import org.pentaho.di.trans.steps.textfileoutput.TextFileOutputMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;
import org.w3c.dom.Node;

import java.util.Map;

@Step( id = "HadoopFileOutputPlugin", image = "HDO.svg", name = "HadoopFileOutputPlugin.Name",
    description = "HadoopFileOutputPlugin.Description",
    documentationUrl = "mk-95pdia003/pdi-transformation-steps/hadoop-file-output",
    categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.BigData",
    i18nPackageName = "org.pentaho.di.trans.steps.hadoopfileoutput" )
@InjectionSupported( localizationPrefix = "HadoopFileOutput.Injection.", groups = { "OUTPUT_FIELDS" } )
public class HadoopFileOutputMeta extends TextFileOutputMeta implements HadoopFileMeta {

  // for message resolution
  private static Class<?> PKG = HadoopFileOutputMeta.class;

  private String sourceConfigurationName;

  private static final String SOURCE_CONFIGURATION_NAME = "source_configuration_name";

  private final NamedClusterService namedClusterService;
  private final RuntimeTestActionService runtimeTestActionService;
  private final RuntimeTester runtimeTester;
  private IMetaStore metaStore;
  private Node embeddedNamedClusterNode;

  public HadoopFileOutputMeta( NamedClusterService namedClusterService,
                               RuntimeTestActionService runtimeTestActionService, RuntimeTester runtimeTester ) {
    this.namedClusterService = namedClusterService;
    this.runtimeTestActionService = runtimeTestActionService;
    this.runtimeTester = runtimeTester;
  }

  @Override
  public void setDefault() {
    // call the base classes method
    super.setDefault();

    // now set the default for the
    // filename to an empty string
    setFileName( "" );
  }

  public String getSourceConfigurationName() {
    return sourceConfigurationName;
  }

  public void setSourceConfigurationName( String ncName ) {
    this.sourceConfigurationName = ncName;
  }

  protected String loadSource( Node stepnode, IMetaStore metastore ) {
    this.metaStore = metastore;
    String url = XMLHandler.getTagValue( stepnode, "file", "name" );
    sourceConfigurationName = XMLHandler.getTagValue( stepnode, "file", SOURCE_CONFIGURATION_NAME );
    embeddedNamedClusterNode = XMLHandler.getSubNode( stepnode, "NamedCluster" );

    return getProcessedUrl( metastore, url );
  }

  protected String getProcessedUrl( IMetaStore metastore, String url ) {
    if ( url == null ) {
      return null;
    }
    if ( metastore == null ) {
      // Maybe we can get a metastore from spoon
      try {
        metaStore = MetaStoreConst.openLocalPentahoMetaStore( false );
      } catch ( Exception e ) {
        // If no local metastore we must ignore and proceed
      }
    } else {
      // if we already have a metastore use it
      metaStore = metastore;
    }
    NamedCluster c = getNamedCluster();
    if ( c != null ) {
      url = c.processURLsubstitution( url, metaStore, new Variables() );
    }
    return url;
  }

  @Override
  public String getClusterName( final String url ) {
    final NamedCluster cluster = getNamedCluster();
    return cluster == null ? null : cluster.getName();
  }


  public NamedCluster getNamedCluster() {
    NamedCluster cluster = namedClusterService.getNamedClusterByName( sourceConfigurationName, metaStore );
    if ( cluster == null ) {
      // Still no metastore, try to make a named cluster from the embedded xml
      if ( namedClusterService.getClusterTemplate() != null ) {
        cluster = namedClusterService.getClusterTemplate().fromXmlForEmbed( embeddedNamedClusterNode );
      }
    }
    return cluster;
  }

  public String getUrlPath( String incomingURL ) {
    return getProcessedUrl( null, incomingURL );
  }

  protected void saveSource( StringBuilder retVal, String fileName ) {
    retVal.append( "      " ).append( XMLHandler.addTagValue( "name", fileName ) );
    retVal.append( "      " ).append( XMLHandler.addTagValue( SOURCE_CONFIGURATION_NAME, sourceConfigurationName ) );
  }

  @Override
  public String getXML() {
    String xml = super.getXML();
    NamedCluster c = namedClusterService.getNamedClusterByName( sourceConfigurationName, metaStore );
    if ( c != null ) {
      xml = xml + c.toXmlForEmbed( "NamedCluster" )  + Const.CR;
    }
    return xml;
  }

  // Receiving metaStore because RepositoryProxy.getMetaStore() returns a hard-coded null
  protected String loadSourceRep( Repository rep, ObjectId id_step,  IMetaStore metaStore ) throws KettleException {
    this.metaStore = metaStore;
    String url = rep.getStepAttributeString( id_step, "file_name" );
    sourceConfigurationName = rep.getStepAttributeString( id_step, SOURCE_CONFIGURATION_NAME );

    return getProcessedUrl( metaStore, url );
  }

  protected void saveSourceRep( Repository rep, ObjectId id_transformation, ObjectId id_step, String fileName )
    throws KettleException {
    rep.saveStepAttribute( id_transformation, id_step, "file_name", fileName );
    rep.saveStepAttribute( id_transformation, id_step, SOURCE_CONFIGURATION_NAME, sourceConfigurationName );
  }

  public NamedClusterService getNamedClusterService() {
    return namedClusterService;
  }

  public RuntimeTester getRuntimeTester() {
    return runtimeTester;
  }

  public RuntimeTestActionService getRuntimeTestActionService() {
    return runtimeTestActionService;
  }

  @Override
  public String exportResources( Bowl executionBowl, Bowl globalManagementBowl, VariableSpace space,
      Map<String, org.pentaho.di.resource.ResourceDefinition> definitions,
      ResourceNamingInterface resourceNamingInterface, Repository repository, IMetaStore metaStore )
      throws KettleException {
    return null;
  }
}
