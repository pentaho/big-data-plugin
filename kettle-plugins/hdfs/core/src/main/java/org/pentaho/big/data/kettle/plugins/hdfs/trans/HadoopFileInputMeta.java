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

import org.apache.commons.lang.Validate;
import org.apache.commons.vfs2.FileName;
import org.pentaho.big.data.impl.cluster.NamedClusterManager;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.bowl.Bowl;
import org.pentaho.di.core.bowl.DefaultBowl;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.fileinput.FileInputList;
import org.pentaho.di.core.fileinput.NonAccessibleFileObject;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.steps.fileinput.text.TextFileInputMeta;
import org.pentaho.hadoop.shim.api.cluster.ClusterInitializationException;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystemLocator;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Map;

import static org.pentaho.big.data.kettle.plugins.hdfs.trans.HadoopFileInputDialog.LOCAL_ENVIRONMENT;
import static org.pentaho.big.data.kettle.plugins.hdfs.trans.HadoopFileInputDialog.S3_ENVIRONMENT;
import static org.pentaho.big.data.kettle.plugins.hdfs.trans.HadoopFileInputDialog.STATIC_ENVIRONMENT;
import static org.pentaho.big.data.kettle.plugins.hdfs.vfs.Schemes.NAMED_CLUSTER_SCHEME;

@Step( id = "HadoopFileInputPlugin", image = "HDI.svg", name = "HadoopFileInputPlugin.Name",
  description = "HadoopFileInputPlugin.Description",
  documentationUrl = "mk-95pdia003/pdi-transformation-steps/hadoop-file-input",
  categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.BigData",
  i18nPackageName = "org.pentaho.di.trans.steps.hadoopfileinput" )
@InjectionSupported( localizationPrefix = "HadoopFileInput.Injection.", groups = { "FILENAME_LINES", "FIELDS",
  "FILTERS" } )
public class HadoopFileInputMeta extends TextFileInputMeta implements HadoopFileMeta {

  // is not used. Can we delete it?

  @SuppressWarnings( "squid:S1068" )
  private VariableSpace variableSpace;

  private Map<String, String> namedClusterURLMapping = null;

  public static final String SOURCE_CONFIGURATION_NAME = "source_configuration_name";
  public static final String LOCAL_SOURCE_FILE = "LOCAL-SOURCE-FILE-";
  public static final String STATIC_SOURCE_FILE = "STATIC-SOURCE-FILE-";
  public static final String S3_SOURCE_FILE = "S3-SOURCE-FILE-";
  public static final String S3_DEST_FILE = "S3-DEST-FILE-";
  private final NamedClusterService namedClusterService;
  private final HadoopFileSystemLocator hadoopFileSystemLocator;
  private final boolean fatalErrorOnHdfsNotFound = "Y".equalsIgnoreCase(
    System.getProperty( Const.KETTLE_FATAL_ERROR_ON_HDFS_NOT_FOUND, Const.KETTLE_FATAL_ERROR_ON_HDFS_NOT_FOUND_DEFAULT ) );

  enum EncryptDirection { ENCRYPT, DECRYPT }

  /**
   * The environment of the selected file/folder
   */
  @Injection( name = "ENVIRONMENT", group = "FILENAME_LINES" )
  public String[] environment = {};

  public HadoopFileInputMeta() {
    this( NamedClusterManager.getInstance(), null );
  }

  public HadoopFileInputMeta( NamedClusterService namedClusterService, HadoopFileSystemLocator hadoopFileSystemLocator ) {
    this.namedClusterService = namedClusterService;
    this.hadoopFileSystemLocator = hadoopFileSystemLocator;
    namedClusterURLMapping = new HashMap<>();
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
        mappings.put( getFriendlyUri( url ).toString(), ncName );
      } catch ( final Exception e ) {
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
      URI friendlyUri = getFriendlyUri( url );
      clusterName = getClusterNameBy( friendlyUri.toString() );
    } catch ( final URISyntaxException e ) {
      // no-op
    }
    return clusterName;
  }

  private URI getFriendlyUri( String url ) throws URISyntaxException {
    URI origUri = new URI( url );
    return new URI( origUri.getScheme(), null, origUri.getHost(), origUri.getPort(),
      origUri.getPath(), origUri.getQuery(), origUri.getFragment() );
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
    return getFileInputList( getParentStepMeta().getParentTransMeta().getBowl(), space );
  }

  @Override
  public FileInputList getFileInputList( Bowl bowl, VariableSpace space ) {
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
      if ( !Utils.isEmpty( source ) ) {
        inputFiles.fileName[ i ] =
          loadUrl( source, sourceNc, getParentStepMeta().getParentTransMeta().getMetaStore(), null );
      } else {
        inputFiles.fileName[ i ] = "";
      }
    }
    FileInputList returnList = createFileList( bowl, space );
    for ( int i = 0; i < inputFiles.fileName.length; i++ ) {
      if ( !canAccessHdfs( inputFiles.fileName[ i ], fatalErrorOnHdfsNotFound ) ) {
        returnList.addNonAccessibleFile( new NonAccessibleFileObject( inputFiles.fileName[ i ] ) );
      }
    }
    return returnList;
  }

  /**
   * If the KETTLE_FATAL_ERROR_ON_HDFS_NOT_FOUND property is set to Y, return false if we can find a named cluster that should
   * be used to access the file AND there is no corresponding HDFS file system for that named cluster.
   *
   * @param fileName
   * @return false if the filename should be accessed via a named cluster and HDFS and it cannot and the KETTLE_FATAL_ERROR_ON_HDFS_NOT_FOUND
   * property is Y
   */
  protected boolean canAccessHdfs( String fileName, boolean checkHdfs ) {
    if ( checkHdfs ) {
      try {
        URI fileUri = new URI( fileName );
        NamedCluster c = namedClusterService.getNamedClusterByHost( fileUri.getHost(), getParentStepMeta().getParentTransMeta().getMetaStore() );
        if ( null == c && NAMED_CLUSTER_SCHEME.equalsIgnoreCase( fileUri.getScheme() ) ) {
          c = namedClusterService.getNamedClusterByName( fileUri.getHost(), getParentStepMeta().getParentTransMeta().getMetaStore() );
        }
        if ( null != c && null == hadoopFileSystemLocator.getHadoopFilesystem( c, fileUri ) ) {
          return false;
        }
      } catch ( URISyntaxException | ClusterInitializationException e ) {
        return false;
      }
    }
    return true;
  }

  FileInputList createFileList( VariableSpace space ) {
    return createFileList( null, space );
  }

  /**
   * Created for test purposes
   */
  FileInputList createFileList( Bowl bowl, VariableSpace space ) {
    return FileInputList.createFileList( bowl, space, inputFiles.fileName, inputFiles.fileMask, inputFiles.excludeFileMask,
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
        String processedPassword;
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
