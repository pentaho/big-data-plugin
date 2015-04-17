/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.job.entries.hadoopcopyfiles;

import java.util.HashMap;
import java.util.Map;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.JobEntry;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.hadoop.HadoopSpoonPlugin;
import org.pentaho.di.core.namedcluster.NamedClusterManager;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.job.entries.copyfiles.JobEntryCopyFiles;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.w3c.dom.Node;

@JobEntry( id = "HadoopCopyFilesPlugin", image = "HDM.svg", name = "HadoopCopyFilesPlugin.Name",
    description = "HadoopCopyFilesPlugin.Description",
    categoryDescription = "i18n:org.pentaho.di.job:JobCategory.Category.BigData",
    i18nPackageName = "org.pentaho.di.job.entries.hadoopcopyfiles" )
public class JobEntryHadoopCopyFiles extends JobEntryCopyFiles {
  
  private Map<String, String> namedClusterURLMapping = null;

  public static final String SOURCE_CONFIGURATION_NAME = "source_configuration_name";
  public static final String SOURCE_FILE_FOLDER = "source_filefolder";
  
  public static final String DESTINATION_CONFIGURATION_NAME = "destination_configuration_name";
  public static final String DESTINATION_FILE_FOLDER = "destination_filefolder";
  private NamedClusterManager namedClusterManager = NamedClusterManager.getInstance();
  
  public JobEntryHadoopCopyFiles() {
    this( "" ); //$NON-NLS-1$
  }

  public JobEntryHadoopCopyFiles( String name ) {
    super( name );
    namedClusterURLMapping = new HashMap<String, String>();
  }

  protected void saveSource( StringBuilder retval, String source ) {
    retval.append( "          " ).append( XMLHandler.addTagValue( "source_filefolder", source ) );    
    String namedCluster = namedClusterURLMapping.get( source );
    retval.append( "          " ).append( XMLHandler.addTagValue( SOURCE_CONFIGURATION_NAME, namedCluster ) );
  } 
 
  protected void saveDestination( StringBuilder retval, String destination ) {
    retval.append( "          " ).append( XMLHandler.addTagValue( "destination_filefolder", destination ) );
    String namedCluster = namedClusterURLMapping.get( destination );
    retval.append( "          " ).append( XMLHandler.addTagValue( DESTINATION_CONFIGURATION_NAME, namedCluster ) );
  }
  
  protected void saveSourceRep( Repository rep, ObjectId id_job, ObjectId id_jobentry, int i, String sourceFileFolder ) throws KettleException {
    rep.saveJobEntryAttribute( id_job, getObjectId(), i, "source_filefolder", sourceFileFolder );
    String namedCluster = namedClusterURLMapping.get( sourceFileFolder );
    rep.saveJobEntryAttribute( id_job, id_jobentry, i, SOURCE_CONFIGURATION_NAME, namedCluster );
  } 
  
  protected void saveDestinationRep( Repository rep, ObjectId id_job, ObjectId id_jobentry, int i, String destinationFileFolder ) throws KettleException {
    rep.saveJobEntryAttribute( id_job, getObjectId(), i, "destination_filefolder", destinationFileFolder );    
    String namedCluster = namedClusterURLMapping.get( destinationFileFolder );
    rep.saveJobEntryAttribute( id_job, id_jobentry, i, DESTINATION_CONFIGURATION_NAME, namedCluster );
  }
  
  protected String loadSourceRep ( Repository rep, ObjectId id_jobentry, int a ) throws KettleException {
    String source_filefolder =  rep.getJobEntryAttributeString( id_jobentry, a, SOURCE_FILE_FOLDER );
    String ncName =  rep.getJobEntryAttributeString( id_jobentry, a, SOURCE_CONFIGURATION_NAME );
    return storeUrl( source_filefolder, ncName );
  }
  
  protected String loadSource ( Node fnode ) {
    String source_filefolder =  XMLHandler.getTagValue( fnode, SOURCE_FILE_FOLDER ); 
    String ncName = XMLHandler.getTagValue( fnode, SOURCE_CONFIGURATION_NAME );
    return storeUrl( source_filefolder, ncName );
  }
  
  protected String loadDestinationRep ( Repository rep, ObjectId id_jobentry, int a ) throws KettleException {
    String destination_filefolder = rep.getJobEntryAttributeString( id_jobentry, a, DESTINATION_FILE_FOLDER );
    String ncName = rep.getJobEntryAttributeString( id_jobentry, a, DESTINATION_CONFIGURATION_NAME );
    return storeUrl( destination_filefolder, ncName );
  }
  
  protected String loadDestination ( Node fnode ) {
    String destination_filefolder =  XMLHandler.getTagValue( fnode, DESTINATION_FILE_FOLDER );
    String ncName = XMLHandler.getTagValue( fnode, DESTINATION_CONFIGURATION_NAME );
    return storeUrl( destination_filefolder, ncName );
  }
  
  
  private String storeUrl(String url, String ncName) {
    url = namedClusterManager.processURLsubstitution( ncName, url, HadoopSpoonPlugin.HDFS_SCHEME );
    if ( !Const.isEmpty( ncName ) && !Const.isEmpty( url ) ) {
      namedClusterURLMapping.put( url, ncName );
    }
    return url;
  }
  
  public void setNamedClusterURLMapping( Map mappings ) {
    this.namedClusterURLMapping = mappings;
  }
  
  public Map<String, String> getNamedClusterURLMapping() {
    return this.namedClusterURLMapping;
  }
}
