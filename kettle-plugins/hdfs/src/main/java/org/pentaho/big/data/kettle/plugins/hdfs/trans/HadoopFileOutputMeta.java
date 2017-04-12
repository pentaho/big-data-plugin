/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.hdfs.trans;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.metastore.MetaStoreConst;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.steps.textfileoutput.TextFileOutputMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;
import org.w3c.dom.Node;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Step( id = "HadoopFileOutputPlugin", image = "HDO.svg", name = "HadoopFileOutputPlugin.Name",
    description = "HadoopFileOutputPlugin.Description",
    documentationUrl = "http://wiki.pentaho.com/display/EAI/Hadoop+File+Output",
    categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.BigData",
    i18nPackageName = "org.pentaho.di.trans.steps.hadoopfileoutput" )
@InjectionSupported( localizationPrefix = "HadoopFileOutput.Injection.", groups = { "OUTPUT_FIELDS" } )
public class HadoopFileOutputMeta extends TextFileOutputMeta {

  // for message resolution
  private static Class<?> PKG = HadoopFileOutputMeta.class;

  private String sourceConfigurationName;

  private static final String SOURCE_CONFIGURATION_NAME = "source_configuration_name";
  private static final String URL_REGEX = "^.*://.*@?.*:[^/]*/";
  private static final Pattern URL_ROOT_PATTERN = Pattern.compile( URL_REGEX );

  private final NamedClusterService namedClusterService;
  private final RuntimeTestActionService runtimeTestActionService;
  private final RuntimeTester runtimeTester;

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
    super.setFileAsCommand( false );
  }

  @Override
  public void setFileAsCommand( boolean fileAsCommand ) {
    // Don't do anything. We want to keep this property as false
    // Throwing a KettleStepException would be desirable but then we
    // need to change the base class' method which is
    // open source.

    throw new RuntimeException( new RuntimeException( BaseMessages.getString( PKG,
        "HadoopFileOutput.MethodNotSupportedException.Message" ) ) );
  }

  public String getSourceConfigurationName() {
    return sourceConfigurationName;
  }

  public void setSourceConfigurationName( String ncName ) {
    this.sourceConfigurationName = ncName;
  }

  protected String loadSource( Node stepnode, IMetaStore metastore ) {
    String url = XMLHandler.getTagValue( stepnode, "file", "name" );
    sourceConfigurationName = XMLHandler.getTagValue( stepnode, "file", SOURCE_CONFIGURATION_NAME );

    return getProcessedUrl( metastore, url );
  }

  protected void saveSource( StringBuilder retVal, String fileName ) {
    retVal.append( "      " ).append( XMLHandler.addTagValue( "name", fileName ) );
    retVal.append( "      " ).append( XMLHandler.addTagValue( SOURCE_CONFIGURATION_NAME, sourceConfigurationName ) );
  }

  protected String getProcessedUrl( IMetaStore metastore, String url ) {
    if ( url == null ) {
      return null;
    }
    IMetaStore metaStore = null;
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
    NamedCluster c = null;
    if ( metaStore != null ) {
      // If we have a metastore get the cluster from it.
      c = namedClusterService.getNamedClusterByName( sourceConfigurationName, metaStore );
    }
    if ( c != null ) {
      String urlPath = getUrlPath( url );
      String rootUrl = getRootURL( c );
      url = urlPath != null && rootUrl != null ? rootUrl + urlPath : url;
    }
    return url;
  }

  // Receiving metaStore because RepositoryProxy.getMetaStore() returns a hard-coded null
  protected String loadSourceRep( Repository rep, ObjectId id_step,  IMetaStore metaStore ) throws KettleException {
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

  protected static String getUrlPath( String incomingURL ) {
    String path = null;
    Matcher matcher = URL_ROOT_PATTERN.matcher( incomingURL );
    if ( matcher.find() ) {
      path = incomingURL.substring( matcher.group().length() );
    }
    return path != null ? path.startsWith( "/" ) ? path : "/" + path : null;
  }

  public static String getRootURL( NamedCluster namedCluster ) {

    String rootURL = null;

    if ( namedCluster == null ) {
      return null;
    }

    String scheme = namedCluster.getShimIdentifier();
    String ncHostname = namedCluster.getHdfsHost() != null ? namedCluster.getHdfsHost().trim() : "";
    String ncPort = namedCluster.getHdfsPort() != null ? namedCluster.getHdfsPort().trim() : "";
    String ncUsername = namedCluster.getHdfsUsername() != null ? namedCluster.getHdfsUsername().trim() : "";
    String ncPassword = namedCluster.getHdfsPassword() != null ? namedCluster.getHdfsPassword().trim() : "";
    if ( ncPort.isEmpty() ) {
      ncPort = "-1";
    }
    scheme = scheme != null ? scheme : namedCluster.isMapr() ? "maprfs" : "hdfs";

    StringBuilder rootURLBuilder = new StringBuilder();
    rootURLBuilder.append( scheme ).append( "://" );
    if ( !ncUsername.isEmpty() ) {
      rootURLBuilder.append( ncUsername );
      if ( !ncPassword.isEmpty() ) {
        rootURLBuilder.append( ":" ).append( ncPassword );
      }
      rootURLBuilder.append( "@" );
    }
    rootURLBuilder.append( ncHostname ).append( ":" );
    rootURLBuilder.append( ncPort );

    return rootURLBuilder.toString();
  }
}
