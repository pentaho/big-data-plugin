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

import org.apache.commons.vfs.FileName;
import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.provider.url.UrlFileNameParser;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.JobEntry;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.hadoop.HadoopSpoonPlugin;
import org.pentaho.di.core.namedcluster.NamedClusterManager;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.job.entries.copyfiles.JobEntryCopyFiles;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.metastore.api.IMetaStore;
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

  public static final String LOCAL_SOURCE_FILE = "LOCAL-SOURCE-FILE-";
  public static final String LOCAL_DEST_FILE = "LOCAL-DEST-FILE-";

  public static final String STATIC_SOURCE_FILE = "STATIC-SOURCE-FILE-";
  public static final String STATIC_DEST_FILE = "STATIC-DEST-FILE-";

  public static final String S3_SOURCE_FILE = "S3-SOURCE-FILE-";
  public static final String S3_DEST_FILE = "S3-DEST-FILE-";
  
  private NamedClusterManager namedClusterManager = NamedClusterManager.getInstance();

  public JobEntryHadoopCopyFiles() {
    this( "" ); //$NON-NLS-1$
  }

  public JobEntryHadoopCopyFiles( String name ) {
    super( name );
    namedClusterURLMapping = new HashMap<String, String>();
  }

  protected void saveSource( StringBuilder retval, String source ) {
    String namedCluster = namedClusterURLMapping.get( source );
    retval.append( "          " ).append( XMLHandler.addTagValue( SOURCE_FILE_FOLDER, source ) );
    retval.append( "          " ).append( XMLHandler.addTagValue( SOURCE_CONFIGURATION_NAME, namedCluster ) );
  }

  protected void saveDestination( StringBuilder retval, String destination ) {
    String namedCluster = namedClusterURLMapping.get( destination );
    retval.append( "          " ).append( XMLHandler.addTagValue( DESTINATION_FILE_FOLDER, destination ) );
    retval.append( "          " ).append( XMLHandler.addTagValue( DESTINATION_CONFIGURATION_NAME, namedCluster ) );
  }

  protected void saveSourceRep( Repository rep, ObjectId id_job, ObjectId id_jobentry, int i, String sourceFileFolder )
    throws KettleException {
    String namedCluster = namedClusterURLMapping.get( sourceFileFolder );
    rep.saveJobEntryAttribute( id_job, getObjectId(), i, SOURCE_FILE_FOLDER, sourceFileFolder );
    rep.saveJobEntryAttribute( id_job, id_jobentry, i, SOURCE_CONFIGURATION_NAME, namedCluster );
  }

  protected void saveDestinationRep( Repository rep, ObjectId id_job, ObjectId id_jobentry, int i,
      String destinationFileFolder ) throws KettleException {
    String namedCluster = namedClusterURLMapping.get( destinationFileFolder );
    rep.saveJobEntryAttribute( id_job, getObjectId(), i, DESTINATION_FILE_FOLDER, destinationFileFolder );
    rep.saveJobEntryAttribute( id_job, id_jobentry, i, DESTINATION_CONFIGURATION_NAME, namedCluster );
  }

  protected String loadSourceRep( Repository rep, ObjectId id_jobentry, int a ) throws KettleException {
    String source_filefolder = rep.getJobEntryAttributeString( id_jobentry, a, SOURCE_FILE_FOLDER );
    String ncName = rep.getJobEntryAttributeString( id_jobentry, a, SOURCE_CONFIGURATION_NAME );
    return loadURL( source_filefolder, ncName, getMetaStore(), namedClusterURLMapping );
  }

  protected String loadSource( Node fnode ) {
    String source_filefolder = XMLHandler.getTagValue( fnode, SOURCE_FILE_FOLDER );
    String ncName = XMLHandler.getTagValue( fnode, SOURCE_CONFIGURATION_NAME );
    return loadURL( source_filefolder, ncName, getMetaStore(), namedClusterURLMapping );
  }

  protected String loadDestinationRep( Repository rep, ObjectId id_jobentry, int a ) throws KettleException {
    String destination_filefolder = rep.getJobEntryAttributeString( id_jobentry, a, DESTINATION_FILE_FOLDER );
    String ncName = rep.getJobEntryAttributeString( id_jobentry, a, DESTINATION_CONFIGURATION_NAME );
    return loadURL( destination_filefolder, ncName, getMetaStore(), namedClusterURLMapping );
  }

  protected String loadDestination( Node fnode ) {
    String destination_filefolder = XMLHandler.getTagValue( fnode, DESTINATION_FILE_FOLDER );
    String ncName = XMLHandler.getTagValue( fnode, DESTINATION_CONFIGURATION_NAME );
    return loadURL( destination_filefolder, ncName, getMetaStore(), namedClusterURLMapping );
  }

  public String loadURL( String url, String ncName, IMetaStore metastore, Map mappings ) {
    url =
        namedClusterManager.processURLsubstitution( ncName, url, HadoopSpoonPlugin.HDFS_SCHEME, metastore,
            getVariables() );
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
}
