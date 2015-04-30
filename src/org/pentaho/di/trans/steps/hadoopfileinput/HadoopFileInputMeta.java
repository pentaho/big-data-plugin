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

package org.pentaho.di.trans.steps.hadoopfileinput;

import java.util.HashMap;
import java.util.Map;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.hadoop.HadoopSpoonPlugin;
import org.pentaho.di.core.namedcluster.NamedClusterManager;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.steps.textfileinput.TextFileInputMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

@Step( id = "HadoopFileInputPlugin", image = "HDI.svg", name = "HadoopFileInputPlugin.Name",
    description = "HadoopFileInputPlugin.Description",
    categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.BigData",
    i18nPackageName = "org.pentaho.di.trans.steps.hadoopfileinput" )
public class HadoopFileInputMeta extends TextFileInputMeta {

  private Map<String, String> namedClusterURLMapping = null;
  private static final String SOURCE_CONFIGURATION_NAME = "source_configuration_name";
  private NamedClusterManager namedClusterManager = NamedClusterManager.getInstance();

  public HadoopFileInputMeta() {
    namedClusterURLMapping = new HashMap<String, String>();
  }

  protected String loadSource( Node filenode, Node filenamenode, int i ) {
    String source_filefolder = XMLHandler.getNodeValue( filenamenode );
    Node sourceNode = XMLHandler.getSubNodeByNr( filenode, SOURCE_CONFIGURATION_NAME, i );
    String source = XMLHandler.getNodeValue( sourceNode );
    return storeUrl( source_filefolder, source );
  }

  protected void saveSource( StringBuffer retVal, String source ) {
    retVal.append( "      " ).append( XMLHandler.addTagValue( "name", source ) );
    String namedCluster = namedClusterURLMapping.get( source );
    retVal.append( "          " ).append( XMLHandler.addTagValue( SOURCE_CONFIGURATION_NAME, namedCluster ) );
  }

  protected String loadSourceRep( Repository rep, ObjectId id_step, int i ) throws KettleException {
    String source_filefolder = rep.getStepAttributeString( id_step, i, "file_name" );
    String ncName = rep.getJobEntryAttributeString( id_step, i, SOURCE_CONFIGURATION_NAME );
    return storeUrl( source_filefolder, ncName );
  }

  protected void saveSourceRep( Repository rep, ObjectId id_transformation, ObjectId id_step, int i, String fileName )
    throws KettleException {
    rep.saveStepAttribute( id_transformation, id_step, i, "file_name", fileName );
    String namedCluster = namedClusterURLMapping.get( fileName );
    rep.saveStepAttribute( id_transformation, id_step, i, SOURCE_CONFIGURATION_NAME, namedCluster );
  }

  private String storeUrl( String url, String ncName ) {
    url = 
        namedClusterManager.processURLsubstitution( 
            ncName, url, HadoopSpoonPlugin.HDFS_SCHEME, getMetaStore(), null );
    if ( !Const.isEmpty( ncName ) && !Const.isEmpty( url ) ) {
      namedClusterURLMapping.put( url, ncName );
    }
    return url;
  }

  private IMetaStore getMetaStore() {
    if ( repository != null ) {
      return repository.getMetaStore();
    }
    return null;
  }

  public void setNamedClusterURLMapping( Map<String, String> mappings ) {
    this.namedClusterURLMapping = mappings;
  }

  public Map<String, String> getNamedClusterURLMapping() {
    return this.namedClusterURLMapping;
  }
}
