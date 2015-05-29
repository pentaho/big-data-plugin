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

import org.apache.commons.vfs.FileName;
import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.provider.url.UrlFileNameParser;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.hadoop.HadoopSpoonPlugin;
import org.pentaho.di.core.namedcluster.NamedClusterManager;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.core.variables.VariableSpace;
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

  private VariableSpace variableSpace;
  private Map<String, String> namedClusterURLMapping = null;

  private static final String SOURCE_CONFIGURATION_NAME = "source_configuration_name";
  public static final String LOCAL_SOURCE_FILE = "LOCAL-SOURCE-FILE-";
  public static final String STATIC_SOURCE_FILE = "STATIC-SOURCE-FILE-";
  public static final String S3_SOURCE_FILE = "S3-SOURCE-FILE-";
  public static final String S3_DEST_FILE = "S3-DEST-FILE-";

  public HadoopFileInputMeta() {
    namedClusterURLMapping = new HashMap<String, String>();
  }

  protected String loadSource( Node filenode, Node filenamenode, int i, IMetaStore metaStore ) {
    String source_filefolder = XMLHandler.getNodeValue( filenamenode );
    Node sourceNode = XMLHandler.getSubNodeByNr( filenode, SOURCE_CONFIGURATION_NAME, i );
    String source = XMLHandler.getNodeValue( sourceNode );
    return loadUrl( source_filefolder, source, metaStore, namedClusterURLMapping );
  }

  protected void saveSource( StringBuffer retVal, String source ) {
    String namedCluster = namedClusterURLMapping.get( source );
    retVal.append( "      " ).append( XMLHandler.addTagValue( "name", source ) );
    retVal.append( "          " ).append( XMLHandler.addTagValue( SOURCE_CONFIGURATION_NAME, namedCluster ) );
  }

  protected String loadSourceRep( Repository rep, ObjectId id_step, int i ) throws KettleException {
    String source_filefolder = rep.getStepAttributeString( id_step, i, "file_name" );
    String ncName = rep.getJobEntryAttributeString( id_step, i, SOURCE_CONFIGURATION_NAME );
    return loadUrl( source_filefolder, ncName, repository != null ? repository.getMetaStore() : null,
        namedClusterURLMapping );
  }

  protected void saveSourceRep( Repository rep, ObjectId id_transformation, ObjectId id_step, int i, String fileName )
    throws KettleException {
    String namedCluster = namedClusterURLMapping.get( fileName );
    rep.saveStepAttribute( id_transformation, id_step, i, "file_name", fileName );
    rep.saveStepAttribute( id_transformation, id_step, i, SOURCE_CONFIGURATION_NAME, namedCluster );
  }

  public String loadUrl( String url, String ncName, IMetaStore metastore, Map<String,String> mappings ) {
    NamedClusterManager namedClusterManager = NamedClusterManager.getInstance();

    NamedCluster c = metastore == null ? null : namedClusterManager.getNamedClusterByName( ncName, metastore );
    if ( c != null && c.isMapr() ) {
      url =
          namedClusterManager.processURLsubstitution( ncName, url, HadoopSpoonPlugin.MAPRFS_SCHEME, metastore,
              variableSpace );
      if ( url != null && !url.startsWith( HadoopSpoonPlugin.MAPRFS_SCHEME ) ) {
        url = HadoopSpoonPlugin.MAPRFS_SCHEME + "://" + url;
      }
    } else if ( !url.startsWith( HadoopSpoonPlugin.MAPRFS_SCHEME ) ) {
      url =
          namedClusterManager.processURLsubstitution( ncName, url, HadoopSpoonPlugin.HDFS_SCHEME, metastore,
              variableSpace );
    }
    if ( !Const.isEmpty( ncName ) && !Const.isEmpty( url ) ) {
      mappings.put( url, ncName );
    }
    return url;
  }

  public void setNamedClusterURLMapping( Map<String, String> mappings ) {
    this.namedClusterURLMapping = mappings;
  }

  public Map<String, String> getNamedClusterURLMapping() {
    return this.namedClusterURLMapping;
  }

  public String getClusterNameBy( String url ) {
    return this.namedClusterURLMapping.get( url );
  }

  public String getUrlPath( String source ) {
    try {
      UrlFileNameParser parser = new UrlFileNameParser();
      FileName fileName = parser.parseUri( null, null, source );
      source = fileName.getPath();
    } catch ( FileSystemException e ) {
      source = null;
    }
    return source;
  }

  public void setVariableSpace( VariableSpace variableSpace ) {
    this.variableSpace = variableSpace;
  }
}
