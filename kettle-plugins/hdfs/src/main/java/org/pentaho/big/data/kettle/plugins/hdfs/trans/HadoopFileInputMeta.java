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

package org.pentaho.big.data.kettle.plugins.hdfs.trans;

import java.net.URISyntaxException;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Map;
import java.net.URI;

import org.apache.commons.lang.Validate;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystemException;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.fileinput.FileInputList;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.steps.fileinput.text.TextFileInputMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;
import org.w3c.dom.Node;

import static org.pentaho.big.data.kettle.plugins.hdfs.trans.HadoopFileInputDialog.LOCAL_ENVIRONMENT;
import static org.pentaho.big.data.kettle.plugins.hdfs.trans.HadoopFileInputDialog.S3_ENVIRONMENT;
import static org.pentaho.big.data.kettle.plugins.hdfs.trans.HadoopFileInputDialog.STATIC_ENVIRONMENT;

@Step( id = "HadoopFileInputPlugin", image = "HDI.svg", name = "HadoopFileInputPlugin.Name",
  description = "HadoopFileInputPlugin.Description",
  documentationUrl = "Products/Hadoop_File_Input",
  categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.BigData",
  i18nPackageName = "org.pentaho.di.trans.steps.hadoopfileinput" )
@InjectionSupported( localizationPrefix = "HadoopFileInput.Injection.", groups = { "FILENAME_LINES", "FIELDS",
  "FILTERS" } )
public class HadoopFileInputMeta extends TextFileInputMeta implements HadoopFileMeta {

  // is not used. Can we delete it?
  private VariableSpace variableSpace;
  private final RuntimeTestActionService runtimeTestActionService;
  private final RuntimeTester runtimeTester;

  private Map<String, String> namedClusterURLMapping = null;

  public static final String SOURCE_CONFIGURATION_NAME = "source_configuration_name";
  public static final String LOCAL_SOURCE_FILE = "LOCAL-SOURCE-FILE-";
  public static final String STATIC_SOURCE_FILE = "STATIC-SOURCE-FILE-";
  public static final String S3_SOURCE_FILE = "S3-SOURCE-FILE-";
  public static final String S3_DEST_FILE = "S3-DEST-FILE-";
  private final NamedClusterService namedClusterService;

  enum EncryptDirection { ENCRYPT, DECRYPT }

  /**
   * The environment of the selected file/folder
   */
  @Injection( name = "ENVIRONMENT", group = "FILENAME_LINES" )
  public String[] environment = {};

  public HadoopFileInputMeta() {
    this( null, null, null );
  }

  public HadoopFileInputMeta( NamedClusterService namedClusterService,
                              RuntimeTestActionService runtimeTestActionService, RuntimeTester runtimeTester ) {
    this.namedClusterService = namedClusterService;
    this.runtimeTestActionService = runtimeTestActionService;
    this.runtimeTester = runtimeTester;
    namedClusterURLMapping = new HashMap<String, String>();
  }

  @Override
  protected String loadSource( Node filenode, Node filenamenode, int i, IMetaStore metaStore ) {
    String source_filefolder = XMLHandler.getNodeValue( filenamenode );
    Node sourceNode = XMLHandler.getSubNodeByNr( filenode, SOURCE_CONFIGURATION_NAME, i );
    String source = XMLHandler.getNodeValue( sourceNode );
    try {
      return source_filefolder == null ? null
        : loadUrl( encryptDecryptPassword( source_filefolder, EncryptDirection.DECRYPT ), source, metaStore,
          namedClusterURLMapping );
    } catch ( Exception ex ) {
      // Do nothing
    }
    return null;
  }

  @Override
  protected void saveSource( StringBuilder retVal, String source ) {
    String namedCluster = namedClusterURLMapping.get( source );
    retVal.append( "      " )
      .append( XMLHandler.addTagValue( "name", encryptDecryptPassword( source, EncryptDirection.ENCRYPT ) ) );
    retVal.append( "          " ).append( XMLHandler.addTagValue( SOURCE_CONFIGURATION_NAME, namedCluster ) );
  }

  // Receiving metaStore because RepositoryProxy.getMetaStore() returns a hard-coded null
  @Override
  protected String loadSourceRep( Repository rep, ObjectId id_step, int i, IMetaStore metaStore )
    throws KettleException {
    String source_filefolder = rep.getStepAttributeString( id_step, i, "file_name" );
    String ncName = rep.getJobEntryAttributeString( id_step, i, SOURCE_CONFIGURATION_NAME );
    return loadUrl( encryptDecryptPassword( source_filefolder, EncryptDirection.DECRYPT ), ncName, metaStore,
      namedClusterURLMapping );
  }

  @Override
  protected void saveSourceRep( Repository rep, ObjectId id_transformation, ObjectId id_step, int i, String fileName )
    throws KettleException {
    String namedCluster = namedClusterURLMapping.get( fileName );
    rep.saveStepAttribute( id_transformation, id_step, i, "file_name",
      encryptDecryptPassword( fileName, EncryptDirection.ENCRYPT ) );
    rep.saveStepAttribute( id_transformation, id_step, i, SOURCE_CONFIGURATION_NAME, namedCluster );
  }

  public String loadUrl( String url, String ncName, IMetaStore metastore, Map<String, String> mappings ) {
    NamedCluster c = namedClusterService.getNamedClusterByName( ncName, metastore );
    if ( c != null ) {
      url = c.processURLsubstitution( url, metastore, new Variables() );
    }
    if ( !Utils.isEmpty( ncName ) && !Utils.isEmpty( url ) && mappings != null ) {
      mappings.put( url, ncName );
      // in addition to the url as-is, add the public uri string version of the url (hidden password) to the map,
      // since that is the value that the data-lineage analyzer will have access to for cluster lookup
      try {
        mappings.put( KettleVFS.getFileObject( url ).getPublicURIString(), ncName );
      } catch ( final KettleFileException e ) {
        // no-op
      }
    }
    return url;
  }

  public void setNamedClusterURLMapping( Map<String, String> mappings ) {
    this.namedClusterURLMapping = mappings;
  }

  public Map<String, String> getNamedClusterURLMapping() {
    return this.namedClusterURLMapping;
  }

  @Override
  public String getClusterName( final String url ) {
    String clusterName = null;
    try {
      clusterName = getClusterNameBy( KettleVFS.getFileObject( url ).getParent().getPublicURIString() );
    } catch ( final KettleFileException | FileSystemException e ) {
      // no-op
    }
    return clusterName;
  }

  public String getClusterNameBy( String url ) {
    return this.namedClusterURLMapping.get( url );
  }

  public String getUrlPath( String incomingURL ) {
    String path = null;
    FileName fileName = getUrlFileName( incomingURL );
    if ( fileName != null ) {
      String root = fileName.getRootURI();
      path = incomingURL.substring( root.length() - 1 );
    }
    return path;
  }

  public void setVariableSpace( VariableSpace variableSpace ) {
    this.variableSpace = variableSpace;
  }

  public NamedClusterService getNamedClusterService() {
    return namedClusterService;
  }

  @Override
  public FileInputList getFileInputList( VariableSpace space ) {
    inputFiles.normalizeAllocation( inputFiles.fileName.length );
    for ( int i = 0; i < environment.length; i++ ) {
      if ( inputFiles.fileName[ i ].contains( "://" ) ) {
        continue;
      }
      String sourceNc = environment[ i ];
      sourceNc = sourceNc.equals( LOCAL_ENVIRONMENT ) ? HadoopFileInputMeta.LOCAL_SOURCE_FILE + i : sourceNc;
      sourceNc = sourceNc.equals( STATIC_ENVIRONMENT ) ? HadoopFileInputMeta.STATIC_SOURCE_FILE + i : sourceNc;
      sourceNc = sourceNc.equals( S3_ENVIRONMENT ) ? HadoopFileInputMeta.S3_SOURCE_FILE + i : sourceNc;
      String source = inputFiles.fileName[ i ];
      if ( !Const.isEmpty( source ) ) {
        inputFiles.fileName[ i ] =
          loadUrl( source, sourceNc, getParentStepMeta().getParentTransMeta().getMetaStore(), null );
      } else {
        inputFiles.fileName[ i ] = "";
      }
    }
    return createFileList( space );
  }

  /**
   * Created for test purposes
   */
  FileInputList createFileList( VariableSpace space ) {
    return FileInputList.createFileList( space, inputFiles.fileName, inputFiles.fileMask, inputFiles.excludeFileMask,
      inputFiles.fileRequired, inputFiles.includeSubFolderBoolean() );
  }

  protected String encryptDecryptPassword( String source, EncryptDirection direction ) {
    Validate.notNull( direction, "'direction' must not be null" );
    try {
      URI uri = new URI( source );
      String userInfo = uri.getUserInfo();
      if ( userInfo != null ) {
        String[] userInfoArray = userInfo.split( ":", 2 );
        if ( userInfoArray.length < 2 ) {
          return source; //no password present
        }
        String password = userInfoArray[ 1 ];
        String processedPassword = password;
        switch ( direction ) {
          case ENCRYPT:
            processedPassword = Encr.encryptPasswordIfNotUsingVariables( password );
            break;
          case DECRYPT:
            processedPassword = Encr.decryptPasswordOptionallyEncrypted( password );
            break;
          default:
            throw new InvalidParameterException( "direction must be 'ENCODE' or 'DECODE'" );
        }
        URI encryptedUri =
          new URI( uri.getScheme(), userInfoArray[ 0 ] + ":" + processedPassword, uri.getHost(), uri.getPort(),
            uri.getPath(), uri.getQuery(), uri.getFragment() );
        return encryptedUri.toString();
      }
    } catch ( URISyntaxException e ) {
      return source; // if this is non-parseable as a uri just return the source without changing it.
    }
    return source; // Just for the compiler should NEVER hit this code
  }
}
