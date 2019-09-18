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

//HadoopClusterDelegateImpl
public class HadoopClusterManager {

  private static final Class<?> PKG = HadoopClusterManager.class;
  public static final String STRING_NAMED_CLUSTERS = BaseMessages.getString( PKG, "NamedClusterDialog.HadoopClusters" );
  private final String fileSeparator = System.getProperty("file.separator");

  private Spoon spoon;
  private NamedClusterService namedClusterService;
  private IMetaStore metaStore;
  private VariableSpace variableSpace;

  public HadoopClusterManager( Spoon spoon, NamedClusterService namedClusterService ) {
    this.spoon = spoon;
    this.namedClusterService = namedClusterService;
    this.metaStore = spoon.getMetaStore();
    this.variableSpace = (AbstractMeta) spoon.getActiveMeta();
  }

  public JSONObject newNamedCluster( String name, String type, String path, String shimVendor, String shimVersion ) {
    NamedCluster nc = namedClusterService.getClusterTemplate();
    try {
      nc.setName( name );
      if ( variableSpace != null ) {
        nc.shareVariablesWith( (VariableSpace) variableSpace );
      } else {
        nc.initializeVariablesFrom( null );
      }
      configureNamedCluster( path, nc, shimVendor, shimVersion );
      saveNamedCluster( metaStore, nc );
      addConfigProperties( nc );
      installSiteFiles( type, path, nc );
      spoon.getShell().getDisplay().asyncExec( () -> spoon.refreshTree( "Hadoop clusters" ) );
    } catch ( Exception e ) {
      return null;
    }

    JSONObject jsonObject = new JSONObject();
    jsonObject.put( "namedCluster", nc.getName() );
    return jsonObject;
  }

  private void configureNamedCluster( String path, NamedCluster nc, String shimVendor, String shimVersion ) throws KettleXMLException, XPathExpressionException,
      UnsupportedEncodingException {

    resolveNamedClusterId( nc, shimVendor, shimVersion );
    Map<String, String> properties = extractSiteFileProperties(path);
    /*
     * Address taken from
     * fs.defaultFS
     * in
     * core-site.xml
     * */
    String hdfsAddress = properties.get( "fs.defaultFS" );
    URI hdfsURL = URI.create( hdfsAddress );
    nc.setHdfsHost( hdfsURL.getHost() );
    nc.setHdfsPort( hdfsURL.getPort() + "" );

    /*
     * Address taken from
     * yarn.resourcemanager.address
     * in
     * yarn-site.xml
     * */
    String jobTrackerAddress = properties.get( "yarn.resourcemanager.address" );
    if ( !jobTrackerAddress.startsWith( "http://" ) ) {
      jobTrackerAddress = "http://" + jobTrackerAddress;
    }
    URI jobTrackerURL = URI.create( jobTrackerAddress  );
    nc.setJobTrackerHost( jobTrackerURL.getHost() );
    nc.setJobTrackerPort( jobTrackerURL.getPort() + "" );

    /*
    * Address and port taken from
    * hive.zookeeper.quorum
    * hive.zookeeper.client.port
    * in
    * hive-site.xml
    * */
    String zooKeeperAddress = properties.get( "hive.zookeeper.quorum" );
    String zooKeeperPort = properties.get( "hive.zookeeper.client.port" );
    nc.setZooKeeperHost( zooKeeperAddress );
    nc.setZooKeeperPort( zooKeeperPort );

    //Site files do not provide the Oozie URL. Where do we get it?
    //What about Kafka?
  }

  private void resolveNamedClusterId( NamedCluster nc, String shimVendor, String shimVersion ) {
    List<ShimIdentifierInterface> shims = getShimIdentifiers();
    for( ShimIdentifierInterface shim : shims ) {
      if ( shim.getVendor().equals( shimVendor ) && shim.getVersion().equals( shimVersion ) ) {
        nc.setShimIdentifier( shim.getId() );
      }
    }
  }

  private Map<String, String> extractSiteFileProperties( String path ) throws KettleXMLException, XPathExpressionException,
      UnsupportedEncodingException {

    Map<String, String> properties = new HashMap();
    path = URLDecoder.decode( path, "UTF-8" );

    Document document = XMLHandler.loadXMLFile( new File (path + "/core-site.xml" ) );
    XPathFactory xpathFactory = XPathFactory.newInstance();
    XPath xpath = xpathFactory.newXPath();
    XPathExpression expr = xpath.compile( "/configuration/property[name='fs.defaultFS']/value/text()" );
    NodeList nodes = (NodeList) expr.evaluate(document, XPathConstants.NODESET);
    properties.put( "fs.defaultFS", nodes.item( 0 ).getNodeValue() );

    document = XMLHandler.loadXMLFile( new File (path + "/yarn-site.xml" ) );
    expr = xpath.compile( "/configuration/property[name='yarn.resourcemanager.address']/value/text()" );
    nodes = (NodeList) expr.evaluate(document, XPathConstants.NODESET);
    properties.put( "yarn.resourcemanager.address", nodes.item( 0 ).getNodeValue() );

    document = XMLHandler.loadXMLFile( new File (path + "/hive-site.xml" ) );
    expr = xpath.compile( "/configuration/property[name='hive.zookeeper.quorum']/value/text()" );
    nodes = (NodeList) expr.evaluate(document, XPathConstants.NODESET);
    properties.put( "hive.zookeeper.quorum", nodes.item( 0 ).getNodeValue() );

    expr = xpath.compile( "/configuration/property[name='hive.zookeeper.client.port']/value/text()" );
    nodes = (NodeList) expr.evaluate(document, XPathConstants.NODESET);
    properties.put( "hive.zookeeper.client.port", nodes.item( 0 ).getNodeValue() );

    return properties;
  }

  private void installSiteFiles( String type, String path, NamedCluster nc ) throws IOException {
    path = URLDecoder.decode( path, "UTF-8" );
    if ( type.equals( "site" ) ) {
      File source = new File( path );
      if ( source.isDirectory() ) {
        File[] files = source.listFiles();
        for ( File file : files ) {
          File destination = new File( getNamedClusterConfigsRootDir() + fileSeparator + nc.getName() );
          FileUtils.copyFileToDirectory( file, destination );
        }
      }
    } else if ( type.equals( "ccfg" ) ) {
      //TODO
    }
  }

  private void saveNamedCluster( IMetaStore metaStore, NamedCluster namedCluster ) {
    try {
      namedClusterService.create( namedCluster, metaStore );
    } catch ( MetaStoreException e ) {
      //TODO
    }
  }

  private void addConfigProperties( NamedCluster namedCluster ) throws IOException {
    Path clusterConfigDirPath = Paths.get( getNamedClusterConfigsRootDir() + fileSeparator + namedCluster.getName() );
    Path
      configPropertiesPath =
      Paths.get( getNamedClusterConfigsRootDir() + fileSeparator + namedCluster.getName() + fileSeparator + "config.properties" );
    Files.createDirectories( clusterConfigDirPath );
    String sampleConfigProperties = namedCluster.getShimIdentifier() + "sampleconfig.properties";
    InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream( sampleConfigProperties );
    if ( inputStream != null ) {
      Files.copy( inputStream, configPropertiesPath, StandardCopyOption.REPLACE_EXISTING );
    }
  }

  public void delNamedCluster( IMetaStore metaStore, NamedCluster namedCluster ) {
    if ( metaStore == null ) {
      metaStore = spoon.getMetaStore();
    }
    deleteNamedCluster( metaStore, namedCluster );
    spoon.refreshTree( STRING_NAMED_CLUSTERS );
    spoon.setShellText();
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

  private void deleteNamedCluster( IMetaStore metaStore, NamedCluster namedCluster ) {
    try {
      if ( namedClusterService.read( namedCluster.getName(), metaStore ) != null ) {
        namedClusterService.delete( namedCluster.getName(), metaStore );
        XmlMetaStore xmlMetaStore = getXmlMetastore( metaStore );
        if ( xmlMetaStore != null ) {
          String path = getNamedClusterConfigsRootDir() + fileSeparator + namedCluster.getName();
          FileUtils.deleteDirectory( new File( path ) );
        }
      }
    } catch ( Exception e ) {
      //TODO
    }
  }

  public List<ShimIdentifierInterface> getShimIdentifiers() {
    return PentahoSystem.getAll( ShimIdentifierInterface.class );
  }

  private String getNamedClusterConfigsRootDir() {
    return System.getProperty( "user.home" ) + File.separator + ".pentaho" + File.separator + "metastore"
      + File.separator + "pentaho" + File.separator + "NamedCluster" + File.separator + "Configs";
  }
}