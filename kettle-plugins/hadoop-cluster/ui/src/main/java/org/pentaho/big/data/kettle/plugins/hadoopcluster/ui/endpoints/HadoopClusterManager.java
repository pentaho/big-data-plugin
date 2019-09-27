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

import org.apache.commons.io.FileUtils;
import org.json.simple.JSONObject;
import org.pentaho.di.base.AbstractMeta;
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
import java.net.URI;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.pentaho.big.data.impl.cluster.tests.Constants.HADOOP_FILE_SYSTEM;
import static org.pentaho.big.data.impl.cluster.tests.Constants.OOZIE;
import static org.pentaho.big.data.impl.cluster.tests.Constants.KAFKA;
import static org.pentaho.big.data.impl.cluster.tests.Constants.ZOOKEEPER;
import static org.pentaho.big.data.impl.cluster.tests.Constants.MAP_REDUCE;

//HadoopClusterDelegateImpl
public class HadoopClusterManager implements RuntimeTestProgressCallback {

  private static final Class<?> PKG = HadoopClusterManager.class;
  public static final String STRING_NAMED_CLUSTERS = BaseMessages.getString( PKG, "NamedClusterDialog.HadoopClusters" );
  private final String fileSeparator = System.getProperty( "file.separator" );
  private static final String BIG_DATA_SHIM = "Pentaho big data shim";
  private static final String PASS = "Pass";
  private static final String WARNING = "Warning";
  private static final String FAIL = "Fail";

  private Spoon spoon;
  private NamedClusterService namedClusterService;
  private IMetaStore metaStore;
  private VariableSpace variableSpace;
  private RuntimeTestStatus runtimeTestStatus = null;
  private static final Logger logChannel = LoggerFactory.getLogger( HadoopClusterManager.class );

  public HadoopClusterManager( Spoon spoon, NamedClusterService namedClusterService ) {
    this.spoon = spoon;
    this.namedClusterService = namedClusterService;
    this.metaStore = spoon.getMetaStore();
    this.variableSpace = (AbstractMeta) spoon.getActiveMeta();
  }

  public JSONObject createNamedCluster( String name, String path, String shimVendor, String shimVersion ) {
    NamedCluster nc = namedClusterService.getClusterTemplate();
    JSONObject jsonObject = new JSONObject();
    try {
      nc.setName( name );
      if ( variableSpace != null ) {
        nc.shareVariablesWith( (VariableSpace) variableSpace );
      } else {
        nc.initializeVariablesFrom( null );
      }
      path = URLDecoder.decode( path, "UTF-8" );
      boolean isConfigurationSet = configureNamedCluster( path, nc, shimVendor, shimVersion );
      if ( isConfigurationSet ) {
        saveNamedCluster( metaStore, nc );
        addConfigProperties( nc );
        installSiteFiles( path, nc );
        if ( spoon.getShell() != null ) {
          spoon.getShell().getDisplay().asyncExec( () -> spoon.refreshTree( "Hadoop clusters" ) );
        }
        jsonObject.put( "namedCluster", nc.getName() );
      } else {
        jsonObject.put( "namedCluster", "" );
      }
    } catch ( Exception e ) {
      logChannel.error( e.getMessage() );
    }
    return jsonObject;
  }

  private boolean configureNamedCluster( String path, NamedCluster nc, String shimVendor, String shimVersion ) {
    resolveNamedClusterId( nc, shimVendor, shimVersion );

    Map<String, String> properties = new HashMap();
    extractProperties( path, "core-site.xml", properties, new String[] { "fs.defaultFS" } );
    extractProperties( path, "yarn-site.xml", properties, new String[] { "yarn.resourcemanager.address" } );
    extractProperties( path, "hive-site.xml", properties,
        new String[] { "hive.zookeeper.quorum", "hive.zookeeper.client.port" } );

    boolean isConfigurationSet = true;
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
    } else {
      isConfigurationSet = false;
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
    } else {
      isConfigurationSet = false;
    }

    /*
     * Address and port taken from
     * hive.zookeeper.quorum
     * hive.zookeeper.client.port
     * in
     * hive-site.xml
     * */
    String zooKeeperAddress = properties.get( "hive.zookeeper.quorum" );
    if ( zooKeeperAddress != null ) {
      String zooKeeperPort = properties.get( "hive.zookeeper.client.port" );
      nc.setZooKeeperHost( zooKeeperAddress );
      nc.setZooKeeperPort( zooKeeperPort );
    } else {
      isConfigurationSet = false;
    }

    //Site files do not provide the Oozie URL. Where do we get it?
    //What about Kafka?
    return isConfigurationSet;
  }

  private void resolveNamedClusterId( NamedCluster nc, String shimVendor, String shimVersion ) {
    List<ShimIdentifierInterface> shims = getShimIdentifiers();
    for ( ShimIdentifierInterface shim : shims ) {
      if ( shim.getVendor().equals( shimVendor ) && shim.getVersion().equals( shimVersion ) ) {
        nc.setShimIdentifier( shim.getId() );
      }
    }
  }

  private void extractProperties( String path, String fileName, Map<String, String> properties, String[] keys ) {
    Document document = parseSiteFileDocument( new File( path + fileSeparator + fileName ) );
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

  private void installSiteFiles( String path, NamedCluster nc ) throws IOException {
    File source = new File( path );
    File destination = new File( getNamedClusterConfigsRootDir() + fileSeparator + nc.getName() );
    if ( source.isDirectory() ) {
      File[] files = source.listFiles();
      for ( File file : files ) {
        if ( file.getName().endsWith( "-site.xml" ) && parseSiteFileDocument( file ) != null ) {
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

  private void saveNamedCluster( IMetaStore metaStore, NamedCluster namedCluster ) {
    try {
      namedClusterService.create( namedCluster, metaStore );
    } catch ( MetaStoreException e ) {
      logChannel.warn( e.getMessage() );
    }
  }

  private void addConfigProperties( NamedCluster namedCluster ) throws IOException {
    Path clusterConfigDirPath = Paths.get( getNamedClusterConfigsRootDir() + fileSeparator + namedCluster.getName() );
    Path
        configPropertiesPath =
        Paths.get( getNamedClusterConfigsRootDir() + fileSeparator + namedCluster.getName() + fileSeparator
            + "config.properties" );
    Files.createDirectories( clusterConfigDirPath );
    String sampleConfigProperties = namedCluster.getShimIdentifier() + "sampleconfig.properties";
    InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream( sampleConfigProperties );
    if ( inputStream != null ) {
      Files.copy( inputStream, configPropertiesPath, StandardCopyOption.REPLACE_EXISTING );
    }
  }

  public void deleteNamedCluster( IMetaStore metaStore, NamedCluster namedCluster ) {
    try {
      if ( namedClusterService.read( namedCluster.getName(), metaStore ) != null ) {
        namedClusterService.delete( namedCluster.getName(), metaStore );
        XmlMetaStore xmlMetaStore = getXmlMetastore( metaStore );
        if ( xmlMetaStore != null ) {
          String path = getNamedClusterConfigsRootDir() + fileSeparator + namedCluster.getName();
          FileUtils.deleteDirectory( new File( path ) );
        }
      }
      spoon.refreshTree( STRING_NAMED_CLUSTERS );
      spoon.setShellText();
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

  public List<ShimIdentifierInterface> getShimIdentifiers() {
    return PentahoSystem.getAll( ShimIdentifierInterface.class );
  }

  public Object runTests( RuntimeTester runtimeTester, String namedClusterName ) {
    NamedCluster nc = namedClusterService.getNamedClusterByName( namedClusterName, this.metaStore );
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
            configureOozieAndKafkaCategories( category, StringUtil.isEmpty( nc.getOozieUrl() ), status );
          } else if ( module.equals( KAFKA ) ) {
            configureOozieAndKafkaCategories( category, StringUtil.isEmpty( nc.getKafkaBootstrapServers() ), status );
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

  private String getNamedClusterConfigsRootDir() {
    return System.getProperty( "user.home" ) + File.separator + ".pentaho" + File.separator + "metastore"
        + File.separator + "pentaho" + File.separator + "NamedCluster" + File.separator + "Configs";
  }
}