/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.trans.steps.hadoopfileoutput;

import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.hadoop.HadoopSpoonPlugin;
import org.pentaho.di.core.namedcluster.NamedClusterManager;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.steps.textfileoutput.TextFileOutputMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

@Step( id = "HadoopFileOutputPlugin", image = "HDO.svg", name = "HadoopFileOutputPlugin.Name",
    description = "HadoopFileOutputPlugin.Description",
    categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.BigData",
    i18nPackageName = "org.pentaho.di.trans.steps.hadoopfileoutput" )
public class HadoopFileOutputMeta extends TextFileOutputMeta {

  // for message resolution
  private static Class<?> PKG = HadoopFileOutputMeta.class;

  private String sourceConfigurationName;

  private static final String SOURCE_CONFIGURATION_NAME = "source_configuration_name";

  private NamedClusterManager namedClusterManager = NamedClusterManager.getInstance();

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

    NamedCluster c = metastore == null ? null :
      namedClusterManager.getNamedClusterByName( sourceConfigurationName, metastore );
    if ( c != null && c.isMapr() ) {
      url =
          namedClusterManager.processURLsubstitution(
              sourceConfigurationName, url, HadoopSpoonPlugin.MAPRFS_SCHEME, metastore, new Variables() );
      if ( url != null && !url.startsWith( HadoopSpoonPlugin.MAPRFS_SCHEME ) ) {
        url = HadoopSpoonPlugin.MAPRFS_SCHEME + "://" + url;
      }
    } else if ( !url.startsWith( HadoopSpoonPlugin.MAPRFS_SCHEME ) ) {
      return namedClusterManager.processURLsubstitution( sourceConfigurationName, url, HadoopSpoonPlugin.HDFS_SCHEME,
          metastore, new Variables() );
    }
    return url;
  }

  protected void saveSource( StringBuffer retVal, String fileName ) {
    retVal.append( "      " ).append( XMLHandler.addTagValue( "name", fileName ) );
    retVal.append( "      " ).append( XMLHandler.addTagValue( SOURCE_CONFIGURATION_NAME, sourceConfigurationName ) );
  }

  protected String loadSourceRep( Repository rep, ObjectId id_step ) throws KettleException {
    String url = rep.getStepAttributeString( id_step, "file_name" );
    sourceConfigurationName = rep.getStepAttributeString( id_step, SOURCE_CONFIGURATION_NAME );

    NamedCluster c = rep.getMetaStore() == null ? null :
      namedClusterManager.getNamedClusterByName( sourceConfigurationName, rep.getMetaStore() );
    if ( c != null && c.isMapr() ) {
      url =
          namedClusterManager.processURLsubstitution(
              sourceConfigurationName, url, HadoopSpoonPlugin.MAPRFS_SCHEME, rep.getMetaStore(), new Variables() );
      if ( url != null && !url.startsWith( HadoopSpoonPlugin.MAPRFS_SCHEME ) ) {
        url = HadoopSpoonPlugin.MAPRFS_SCHEME + "://" + url;
      }
    } else if ( !url.startsWith( HadoopSpoonPlugin.MAPRFS_SCHEME ) ) {
      return namedClusterManager.processURLsubstitution( sourceConfigurationName, url, HadoopSpoonPlugin.HDFS_SCHEME,
          rep.getMetaStore(), new Variables() );
    }
    return url;
  }

  protected void saveSourceRep( Repository rep, ObjectId id_transformation, ObjectId id_step, String fileName )
    throws KettleException {
    rep.saveStepAttribute( id_transformation, id_step, "file_name", fileName );
    rep.saveStepAttribute( id_transformation, id_step, SOURCE_CONFIGURATION_NAME, sourceConfigurationName );
  }
}
