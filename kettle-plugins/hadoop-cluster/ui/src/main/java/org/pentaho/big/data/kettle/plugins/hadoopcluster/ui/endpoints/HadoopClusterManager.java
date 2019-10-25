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

package org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.endpoints;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FileUtils;
import org.json.simple.JSONObject;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.HadoopClusterDialog;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.model.ThinNameClusterModel;
import org.pentaho.big.data.plugins.common.ui.HadoopClusterDelegateImpl;
import org.pentaho.di.base.AbstractMeta;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.hadoop.shim.api.ShimIdentifierInterface;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.stores.delegate.DelegatingMetaStore;
import org.pentaho.metastore.stores.xml.XmlMetaStore;
import org.pentaho.platform.engine.core.system.PentahoSystem;
import org.pentaho.runtime.test.RuntimeTest;
import org.pentaho.runtime.test.RuntimeTestProgressCallback;
import org.pentaho.runtime.test.RuntimeTestStatus;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.module.RuntimeTestModuleResults;
import org.pentaho.runtime.test.result.RuntimeTestResult;
import org.pentaho.runtime.test.result.RuntimeTestResultEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.pentaho.big.data.impl.cluster.tests.Constants.HADOOP_FILE_SYSTEM;
import static org.pentaho.big.data.impl.cluster.tests.Constants.OOZIE;
import static org.pentaho.big.data.impl.cluster.tests.Constants.KAFKA;
import static org.pentaho.big.data.impl.cluster.tests.Constants.ZOOKEEPER;
import static org.pentaho.big.data.impl.cluster.tests.Constants.MAP_REDUCE;

//HadoopClusterDelegateImpl
public class HadoopClusterManager implements RuntimeTestProgressCallback {

  private static final Class<?> PKG = HadoopClusterDialog.class;
  public static final String STRING_NAMED_CLUSTERS_THIN = BaseMessages.getString( PKG, "HadoopClusterTree.Title" );
  public static final String STRING_NAMED_CLUSTERS = BaseMessages.getString( PKG, "HadoopCluster.dialog.title" );
  private final String fileSeparator = System.getProperty( "file.separator" );
  private static final String BIG_DATA_SHIM = "Pentaho big data shim";
  private static final String PASS = "Pass";
  private static final String WARNING = "Warning";
  private static final String FAIL = "Fail";
  private static final String NAMED_CLUSTER = "namedCluster";
  private static final String INSTALLED = "installed";
  private final String internalShim;

  @VisibleForTesting Supplier<List<ShimIdentifierInterface>> shimIdentifiersSupplier =
    () -> PentahoSystem.getAll( ShimIdentifierInterface.class );

  private final Spoon spoon;
  private final NamedClusterService namedClusterService;
  private final IMetaStore metaStore;
  private final VariableSpace variableSpace;

  private RuntimeTestStatus runtimeTestStatus = null;
  private static final Logger logChannel = LoggerFactory.getLogger( HadoopClusterManager.class );

  public HadoopClusterManager( Spoon spoon, NamedClusterService namedClusterService, IMetaStore metaStore,
                               String internalShim ) {
    this.spoon = spoon;
    this.namedClusterService = namedClusterService;
    this.metaStore = metaStore != null ? metaStore : spoon.getMetaStore();
    this.variableSpace = (AbstractMeta) spoon.getActiveMeta();
    this.internalShim = internalShim;
  }

  private File decodeSiteFilesSource( String source ) throws UnsupportedEncodingException {
    if ( !StringUtil.isEmpty( source ) ) {
      source = URLDecoder.decode( source, "UTF-8" );
      return new File( source );
    }
    return new File( "" );
  }

  public JSONObject importNamedCluster( ThinNameClusterModel model ) {
    JSONObject response = new JSONObject();
    response.put( NAMED_CLUSTER, "" );
    try {
      // Validate against using an existing name.
      if ( !isNameValid( model.getName() ) ) {
        return response;
      }
      NamedCluster nc = namedClusterService.getClusterTemplate();
      nc.setName( model.getName() );
      nc.setHdfsUsername( model.getHdfsUsername() );
      nc.setHdfsPassword( model.getHdfsPassword() );
      if ( variableSpace != null ) {
        nc.shareVariablesWith( variableSpace );
      } else {
        nc.initializeVariablesFrom( null );
      }

      File siteFilesSource = decodeSiteFilesSource( model.getImportPath() );
      if ( siteFilesSource.exists() ) {
        boolean
          isConfigurationSet =
          configureNamedCluster( siteFilesSource, nc, model.getShimVendor(), model.getShimVersion() );
        if ( isConfigurationSet ) {
          namedClusterService.create( nc, metaStore );
          installSiteFiles( siteFilesSource, nc );
          createConfigProperties( nc );
          refreshTree();
          response.put( NAMED_CLUSTER, nc.getName() );
        }
      }
    } catch ( Exception e ) {
      logChannel.error( e.getMessage() );
    }
    return response;
  }

  private NamedCluster createXMLSchema( ThinNameClusterModel model ) throws MetaStoreException {
    NamedCluster nc = namedClusterService.getClusterTemplate();
    nc.setName( model.getName() );
    nc.setHdfsHost( model.getHdfsHost() );
    nc.setHdfsPort( model.getHdfsPort() );
    nc.setHdfsUsername( model.getHdfsUsername() );
    nc.setHdfsPassword( model.getHdfsPassword() );
    nc.setJobTrackerHost( model.getJobTrackerHost() );
    nc.setJobTrackerPort( model.getJobTrackerPort() );
    nc.setZooKeeperHost( model.getZooKeeperHost() );
    nc.setZooKeeperPort( model.getZooKeeperPort() );
    nc.setOozieUrl( model.getOozieUrl() );
    nc.setKafkaBootstrapServers( model.getKafkaBootstrapServers() );
    resolveShimIdentifier( nc, model.getShimVendor(), model.getShimVersion() );
    if ( variableSpace != null ) {
      nc.shareVariablesWith( variableSpace );
    } else {
      nc.initializeVariablesFrom( null );
    }
    namedClusterService.create( nc, metaStore );
    return nc;
  }

  public JSONObject createNamedCluster( ThinNameClusterModel model ) {
    JSONObject response = new JSONObject();
    response.put( NAMED_CLUSTER, "" );
    try {
      // Validate against using an existing name.
      if ( !isNameValid( model.getName() ) ) {
        return response;
      }

      NamedCluster nc = createXMLSchema( model );
      File siteFilesSource = decodeSiteFilesSource( model.getImportPath() );
      if ( siteFilesSource.exists() ) {
        installSiteFiles( siteFilesSource, nc );
      }
      createConfigProperties( nc );
      refreshTree();
      response.put( NAMED_CLUSTER, nc.getName() );
    } catch ( Exception e ) {
      logChannel.error( e.getMessage() );
    }
    return response;
  }

  private boolean isNameValid( String name ) throws MetaStoreException {
    boolean isValid = true;
    if ( namedClusterService.contains( name, metaStore ) ) {
      logChannel.error(
        "Invalid name. A Named Cluster with the same name already exists. A different name must be provided:{}",
        name );
      isValid = false;
    }
    return isValid;
  }

  public JSONObject editNamedCluster( ThinNameClusterModel model, boolean isEditMode ) {
    JSONObject response = new JSONObject();
    response.put( NAMED_CLUSTER, "" );
    try {
      // Validate against using an existing name when performing a duplicate operation.
      if ( !isEditMode && !isNameValid( model.getName() ) ) {
        return response;
      }
      // Validate against using an existing name when performing an edit operation.
      if ( isEditMode && !model.getName().equals( model.getOldName() ) && !isNameValid( model.getName() ) ) {
        return response;
      }

      // Must get the current shim identifier before the creation of the Named Cluster xml schema for later comparison.
      String shimId = namedClusterService.getNamedClusterByName( model.getOldName(), metaStore ).getShimIdentifier();

      // Create new or update existing Named Cluster XML schema.
      NamedCluster nc = createXMLSchema( model );

      File oldConfigFolder = new File( getNamedClusterConfigsRootDir() + fileSeparator + model.getOldName() );
      File newConfigFolder = new File( getNamedClusterConfigsRootDir() + fileSeparator + nc.getName() );

      // Copy all files from the old config folder to the new config folder.
      if ( !oldConfigFolder.equals( newConfigFolder ) ) {
        FileUtils.copyDirectory( oldConfigFolder, newConfigFolder );
      }

      // If source provided, install site files in the new config folder deleting all existing.
      File siteFilesSource = decodeSiteFilesSource( model.getImportPath() );
      if ( siteFilesSource.exists() ) {
        installSiteFiles( siteFilesSource, nc );
      }

      // If the user changed the shim, create a new config.properties file that corresponds to that shim
      // in the new config folder.
      if ( nc.getShimIdentifier() != null && !nc.getShimIdentifier().equals( shimId ) ) {
        createConfigProperties( nc );
      }

      // Delete old config folder.
      if ( isEditMode && !oldConfigFolder.equals( newConfigFolder ) ) {
        deleteNamedCluster( metaStore, model.getOldName(), false );
      }

      refreshTree();
      response.put( NAMED_CLUSTER, nc.getName() );
    } catch ( Exception e ) {
      logChannel.error( e.getMessage() );
    }
    return response;
  }

  public ThinNameClusterModel getNamedCluster( String namedCluster ) {
    NamedCluster nc = namedClusterService.getNamedClusterByName( namedCluster, metaStore );
    ThinNameClusterModel model = null;
    if ( nc != null ) {
      model = new ThinNameClusterModel();
      model.setName( nc.getName() );
      model.setHdfsHost( nc.getHdfsHost() );
      model.setHdfsUsername( nc.getHdfsUsername() );
      model.setHdfsPassword( nc.getHdfsPassword() );
      model.setHdfsPort( nc.getHdfsPort() );
      model.setJobTrackerHost( nc.getJobTrackerHost() );
      model.setJobTrackerPort( nc.getJobTrackerPort() );
      model.setKafkaBootstrapServers( nc.getKafkaBootstrapServers() );
      model.setOozieUrl( nc.getOozieUrl() );
      model.setZooKeeperPort( nc.getZooKeeperPort() );
      model.setZooKeeperHost( nc.getZooKeeperHost() );
      resolveShimVendorAndVersion( model, nc.getShimIdentifier() );
    }
    return model;
  }

  private boolean configureNamedCluster( File importPath, NamedCluster nc, String shimVendor, String shimVersion ) {
    resolveShimIdentifier( nc, shimVendor, shimVersion );

    String oozieBaseUrl = "oozie.base.url";
    Map<String, String> properties = new HashMap();
    extractProperties( importPath, "core-site.xml", properties, new String[] { "fs.defaultFS" } );
    extractProperties( importPath, "yarn-site.xml", properties, new String[] { "yarn.resourcemanager.address" } );
    extractProperties( importPath, "hive-site.xml", properties,
      new String[] { "hive.zookeeper.quorum", "hive.zookeeper.client.port" } );
    extractProperties( importPath, "oozie-site.xml", properties, new String[] { oozieBaseUrl } );
    if ( properties.get( oozieBaseUrl ) == null ) {
      extractProperties( importPath, "oozie-default.xml", properties, new String[] { oozieBaseUrl } );
    }

    boolean isConfigurationSet = false;
    /*
     * Address taken from
     * fs.defaultFS
     * in
     * core-site.xml
     * */
    String hdfsAddress = properties.get( "fs.defaultFS" );
    if ( hdfsAddress != null ) {
      URI hdfsURL = URI.create( hdfsAddress );
      nc.setHdfsHost( hdfsURL.getHost() );
      nc.setHdfsPort( hdfsURL.getPort() + "" );
      isConfigurationSet = true;
    }

    /*
     * Address taken from
     * yarn.resourcemanager.address
     * in
     * yarn-site.xml
     * */
    String jobTrackerAddress = properties.get( "yarn.resourcemanager.address" );
    if ( jobTrackerAddress != null ) {
      if ( !jobTrackerAddress.startsWith( "http://" ) ) {
        jobTrackerAddress = "http://" + jobTrackerAddress;
      }
      URI jobTrackerURL = URI.create( jobTrackerAddress );
      nc.setJobTrackerHost( jobTrackerURL.getHost() );
      nc.setJobTrackerPort( jobTrackerURL.getPort() + "" );
      isConfigurationSet = true;
    }

    /*
     * Address and port taken from
     * hive.zookeeper.quorum
     * hive.zookeeper.client.port
     * in
     * hive-site.xml
     * */
    String zooKeeperAddress = properties.get( "hive.zookeeper.quorum" );
    String zooKeeperPort = properties.get( "hive.zookeeper.client.port" );
    if ( zooKeeperAddress != null && zooKeeperPort != null ) {
      nc.setZooKeeperHost( zooKeeperAddress );
      nc.setZooKeeperPort( zooKeeperPort );
      isConfigurationSet = true;
    }

    /*
     * Address and port taken from
     * oozie.base.url
     * in
     * oozie-site.xml
     * if it does not exist then it is taken from
     * oozie-default.xml
     * */
    String oozieAddress = properties.get( oozieBaseUrl );
    if ( oozieAddress != null ) {
      nc.setOozieUrl( oozieAddress );
      isConfigurationSet = true;
    }

    return isConfigurationSet;
  }

  private void resolveShimIdentifier( NamedCluster nc, String shimVendor, String shimVersion ) {
    List<ShimIdentifierInterface> shims = getShimIdentifiers();
    for ( ShimIdentifierInterface shim : shims ) {
      if ( shim.getVendor().equals( shimVendor ) && shim.getVersion().equals( shimVersion ) ) {
        nc.setShimIdentifier( shim.getId() );
      }
    }
  }

  private void resolveShimVendorAndVersion( ThinNameClusterModel model, String shimIdentifier ) {
    List<ShimIdentifierInterface> shims = getShimIdentifiers();
    for ( ShimIdentifierInterface shim : shims ) {
      if ( shim.getId().equals( shimIdentifier ) ) {
        model.setShimVersion( shim.getVersion() );
        model.setShimVendor( shim.getVendor() );
      }
    }
  }

  private void extractProperties( File importPath, String fileName, Map<String, String> properties, String[] keys ) {
    File siteFile = new File( importPath, fileName );
    if ( siteFile.exists() ) {
      Document document = parseSiteFileDocument( new File( importPath, fileName ) );
      if ( document != null ) {
        XPathFactory xpathFactory = XPathFactory.newInstance();
        XPath xpath = xpathFactory.newXPath();
        for ( String key : keys ) {
          try {
            XPathExpression expr = xpath.compile( "/configuration/property[name='" + key + "']/value/text()" );
            NodeList nodes = (NodeList) expr.evaluate( document, XPathConstants.NODESET );
            if ( nodes.getLength() > 0 ) {
              properties.put( key, nodes.item( 0 ).getNodeValue() );
            }
          } catch ( XPathExpressionException e ) {
            logChannel.warn( e.getMessage() );
          }
        }
      }
    }
  }

  @SuppressWarnings( "javasecurity:S2083" )
  public JSONObject installDriver( String source ) {
    boolean success = true;
    try {
      String destination = System.getProperties().getProperty( Const.SHIM_DRIVER_DEPLOYMENT_LOCATION, "./" );
      FileUtils.copyFileToDirectory( new File( source ), new File( destination ) );
    } catch ( IOException e ) {
      success = false;
      logChannel.error( e.getMessage() );
    }
    JSONObject response = new JSONObject();
    response.put( INSTALLED, success );
    return response;
  }

  private void installSiteFiles( File source, NamedCluster nc ) throws IOException {
    if ( source.isDirectory() ) {
      File destination = new File( getNamedClusterConfigsRootDir() + fileSeparator + nc.getName() );
      File[] files = source.listFiles();
      for ( File file : files ) {
        if ( ( file.getName().endsWith( "-site.xml" ) || file.getName().endsWith( "-default.xml" ) || file.getName()
          .equals( "config.properties" ) ) && parseSiteFileDocument( file ) != null ) {
          FileUtils.copyFileToDirectory( file, destination );
        }
      }
    }
  }

  private Document parseSiteFileDocument( File file ) {
    Document document = null;
    try {
      document = XMLHandler.loadXMLFile( file );
    } catch ( KettleXMLException e ) {
      logChannel.warn( String.format( "Site file %s is not a well formed XML document", file.getName() ) );

    }
    return document;
  }

  private void createConfigProperties( NamedCluster namedCluster ) throws IOException {
    Path clusterConfigDirPath = Paths.get( getNamedClusterConfigsRootDir() + fileSeparator + namedCluster.getName() );
    Path
      configPropertiesPath =
      Paths.get( getNamedClusterConfigsRootDir() + fileSeparator + namedCluster.getName() + fileSeparator
        + "config.properties" );
    Files.createDirectories( clusterConfigDirPath );
    String sampleConfigProperties = namedCluster.getShimIdentifier() + "sampleconfig.properties";
    InputStream
      inputStream =
      HadoopClusterDelegateImpl.class.getClassLoader().getResourceAsStream( sampleConfigProperties );
    if ( inputStream != null ) {
      Files.copy( inputStream, configPropertiesPath, StandardCopyOption.REPLACE_EXISTING );
    }
  }

  public void deleteNamedCluster( IMetaStore metaStore, String namedCluster, boolean refreshTree ) {
    try {
      if ( namedClusterService.read( namedCluster, metaStore ) != null ) {
        namedClusterService.delete( namedCluster, metaStore );
        XmlMetaStore xmlMetaStore = getXmlMetastore( metaStore );
        if ( xmlMetaStore != null ) {
          String path = getNamedClusterConfigsRootDir() + fileSeparator + namedCluster;
          FileUtils.deleteDirectory( new File( path ) );
        }
      }
      if ( refreshTree ) {
        refreshTree();
      }
    } catch ( Exception e ) {
      logChannel.warn( e.getMessage() );
    }
  }

  private XmlMetaStore getXmlMetastore( IMetaStore metaStore ) throws MetaStoreException {
    XmlMetaStore xmlMetaStore = null;
    if ( metaStore instanceof DelegatingMetaStore ) {
      IMetaStore activeMetastore = ( (DelegatingMetaStore) metaStore ).getActiveMetaStore();
      if ( activeMetastore instanceof XmlMetaStore ) {
        xmlMetaStore = (XmlMetaStore) activeMetastore;
      }
    } else if ( metaStore instanceof XmlMetaStore ) {
      xmlMetaStore = (XmlMetaStore) metaStore;
    }
    return xmlMetaStore;
  }

  /**
   * @return shim identifiers, excluding the internal shim, which should not be exposed to the cluster ui.
   */
  List<ShimIdentifierInterface> getShimIdentifiers() {
    return shimIdentifiersSupplier.get().stream()
      .filter( s -> !internalShim.equals( s.getId() ) )
      .collect( Collectors.toList() );
  }

  public Object runTests( RuntimeTester runtimeTester, String namedCluster ) {
    NamedCluster nc = namedClusterService.getNamedClusterByName( namedCluster, this.metaStore );
    if ( nc != null ) {
      try {
        if ( runtimeTester != null ) {
          runtimeTestStatus = null;
          runtimeTester.runtimeTest( nc, this );
          synchronized ( this ) {
            while ( runtimeTestStatus == null ) {
              wait();
            }
          }
        }
      } catch ( Exception e ) {
        logChannel.warn( e.getLocalizedMessage() );
      }
      return produceTestCategories( runtimeTestStatus, nc );
    } else {
      return "[]";
    }
  }

  private Object[] produceTestCategories( RuntimeTestStatus runtimeTestStatus, NamedCluster nc ) {

    HashMap<String, TestCategory> categories = new HashMap<>();
    categories.put( HADOOP_FILE_SYSTEM, new TestCategory( "Hadoop file system" ) );
    categories.put( OOZIE, new TestCategory( "Oozie host connection" ) );
    categories.put( KAFKA, new TestCategory( "Kafka connection" ) );
    categories.put( ZOOKEEPER, new TestCategory( "Zookeeper connection" ) );
    categories.put( MAP_REDUCE, new TestCategory( "Job tracker / resource manager" ) );
    categories.put( BIG_DATA_SHIM, new TestCategory( BIG_DATA_SHIM ) );

    if ( runtimeTestStatus != null && nc != null ) {
      for ( RuntimeTestModuleResults moduleResults : runtimeTestStatus.getModuleResults() ) {
        for ( RuntimeTestResult testResult : moduleResults.getRuntimeTestResults() ) {
          RuntimeTest runtimeTest = testResult.getRuntimeTest();
          String name = runtimeTest.getName();
          String status = getTestStatus( testResult.getOverallStatusEntry() );
          String module = runtimeTest.getModule();
          Category category = categories.get( module );
          category.setCategoryActive( true );

          if ( module.equals( HADOOP_FILE_SYSTEM ) ) {
            Test test = new Test( name );
            test.setTestStatus( status );
            test.setTestActive( true );
            category.addTest( test );
            configureHadoopFileSystemCategory( category, status );
          } else if ( module.equals( OOZIE ) ) {
            configureOozieAndKafkaCategories( category, !StringUtil.isEmpty( nc.getOozieUrl() ), status );
          } else if ( module.equals( KAFKA ) ) {
            configureOozieAndKafkaCategories( category, !StringUtil.isEmpty( nc.getKafkaBootstrapServers() ), status );
          } else {
            category.setCategoryStatus( status );
          }
        }
      }
    }
    return categories.values().toArray();
  }

  private void configureHadoopFileSystemCategory( Category category, String status ) {
    String currentStatus = category.getCategoryStatus();
    if ( status.equals( FAIL ) || ( status.equals( WARNING ) && !currentStatus.equals( FAIL ) ) || (
      status.equals( PASS ) && StringUtil.isEmpty( currentStatus ) ) ) {
      category.setCategoryStatus( status );
    }
  }

  private void configureOozieAndKafkaCategories( Category category, boolean isActive, String status ) {
    category.setCategoryActive( isActive );
    if ( category.isCategoryActive() ) {
      category.setCategoryStatus( status );
    }
  }

  private String getTestStatus( RuntimeTestResultEntry summary ) {
    String status = "";
    switch ( summary.getSeverity() ) {
      case INFO:
        status = PASS;
        break;
      case SKIPPED:
        status = WARNING;
        break;
      case FATAL:
        status = FAIL;
        break;
      case ERROR:
        status = FAIL;
        break;
      default:
        break;
    }
    return status;
  }

  public void onProgress( final RuntimeTestStatus clusterTestStatus ) {
    synchronized ( this ) {
      if ( clusterTestStatus.isDone() ) {
        runtimeTestStatus = clusterTestStatus;
        notifyAll();
      }
    }
  }

  private void refreshTree() {
    if ( spoon.getShell() != null ) {
      //TODO Refreshing the "Hadoop clusters" tree item will go away when the SWT code is removed.
      spoon.getShell().getDisplay().asyncExec( () -> spoon.refreshTree( STRING_NAMED_CLUSTERS ) );
      spoon.getShell().getDisplay().asyncExec( () -> spoon.refreshTree( STRING_NAMED_CLUSTERS_THIN ) );
    }
  }

  private String getNamedClusterConfigsRootDir() {
    return System.getProperty( "user.home" ) + File.separator + ".pentaho" + File.separator + "metastore"
      + File.separator + "pentaho" + File.separator + "NamedCluster" + File.separator + "Configs";
  }
}
