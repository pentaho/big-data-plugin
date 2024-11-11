/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2024 by Hitachi Vantara : http://www.pentaho.com
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
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.fileupload2.core.FileItem;
import org.apache.commons.io.FileUtils;
import org.json.simple.JSONObject;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.HadoopClusterDialog;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.model.ThinNameClusterModel;
import org.pentaho.big.data.plugins.common.ui.HadoopClusterDelegateImpl;
import org.pentaho.di.base.AbstractMeta;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.osgi.api.NamedClusterSiteFile;
import org.pentaho.di.core.osgi.impl.NamedClusterSiteFileImpl;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.hadoop.shim.api.core.ShimIdentifierInterface;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.platform.engine.core.system.PentahoSystem;
import org.pentaho.runtime.test.RuntimeTest;
import org.pentaho.runtime.test.RuntimeTestProgressCallback;
import org.pentaho.runtime.test.RuntimeTestStatus;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.module.RuntimeTestModuleResults;
import org.pentaho.runtime.test.result.RuntimeTestResult;
import org.pentaho.runtime.test.result.RuntimeTestResultEntry;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;


import static org.pentaho.big.data.impl.cluster.tests.Constants.HADOOP_FILE_SYSTEM;
import static org.pentaho.big.data.impl.cluster.tests.Constants.OOZIE;
import static org.pentaho.big.data.impl.cluster.tests.Constants.KAFKA;
import static org.pentaho.big.data.impl.cluster.tests.Constants.ZOOKEEPER;
import static org.pentaho.big.data.impl.cluster.tests.Constants.MAP_REDUCE;
import static org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.model.ThinNameClusterModel.NAME_KEY;

//HadoopClusterDelegateImpl
public class HadoopClusterManager implements RuntimeTestProgressCallback {

  private static final Class<?> PKG = HadoopClusterDialog.class;
  public static final String STRING_NAMED_CLUSTERS = BaseMessages.getString( PKG, "HadoopClusterTree.Title" );
  public static final String PLACEHOLDER_VALUE = "[object Object]";
  private final String fileSeparator = System.getProperty( "file.separator" );
  private static final String PASS = "Pass";
  private static final String WARNING = "Warning";
  private static final String FAIL = "Fail";
  private static final String NAMED_CLUSTER = "namedCluster";
  private static final String INSTALLED = "installed";
  private static final String CONFIG_PROPERTIES = "config.properties";
  private static final String KEYTAB_AUTH_FILE = "keytabAuthFile";
  private static final String KEYTAB_IMPL_FILE = "keytabImpFile";
  public static final String MAPR_SHIM = "Map-R";
  public static final String MAPRFS_SCHEME = "maprfs";

  private static final LogChannelInterface log =
    KettleLogStore.getLogChannelInterfaceFactory().create( "HadoopClusterManager" );

  private final String internalShim;

  private enum KERBEROS_SUBTYPE {
    PASSWORD( "Password" ),
    KEYTAB( "Keytab" );

    private String val;

    KERBEROS_SUBTYPE( String val ) {
      this.val = val;
    }

    public String getValue() {
      return this.val;
    }
  }

  private enum SECURITY_TYPE {
    NONE( "None" ),
    KERBEROS( "Kerberos" ),
    KNOX( "Knox" );

    private String val;

    SECURITY_TYPE( String val ) {
      this.val = val;
    }

    public String getValue() {
      return this.val;
    }
  }

  private enum IMPERSONATION_TYPE {
    SIMPLE( "simple" ),
    DISABLED( "disabled" );

    private String val;

    IMPERSONATION_TYPE( String val ) {
      this.val = val;
    }

    public String getValue() {
      return this.val;
    }
  }

  private static final String KERBEROS_AUTHENTICATION_USERNAME = "pentaho.authentication.default.kerberos.principal";
  private static final String KERBEROS_AUTHENTICATION_PASS = "pentaho.authentication.default.kerberos.password";
  private static final String KERBEROS_IMPERSONATION_USERNAME =
    "pentaho.authentication.default.mapping.server.credentials.kerberos.principal";
  private static final String KERBEROS_IMPERSONATION_PASS =
    "pentaho.authentication.default.mapping.server.credentials.kerberos.password";
  private static final String IMPERSONATION = "pentaho.authentication.default.mapping.impersonation.type";
  private static final String KEYTAB_AUTHENTICATION_LOCATION = "pentaho.authentication.default.kerberos.keytabLocation";
  private static final String KEYTAB_IMPERSONATION_LOCATION =
    "pentaho.authentication.default.mapping.server.credentials.kerberos.keytabLocation";

  @VisibleForTesting Supplier<List<ShimIdentifierInterface>> shimIdentifiersSupplier =
    () -> PentahoSystem.getAll( ShimIdentifierInterface.class );

  private final Spoon spoon;
  private final NamedClusterService namedClusterService;
  private final IMetaStore metaStore;
  private final VariableSpace variableSpace;
  private RuntimeTestStatus runtimeTestStatus = null;

  public HadoopClusterManager( Spoon spoon, NamedClusterService namedClusterService, IMetaStore metaStore,
                               String internalShim ) {
    this.spoon = spoon;
    this.namedClusterService = namedClusterService;
    this.metaStore = metaStore != null ? metaStore : spoon.getMetaStore();
    this.variableSpace = spoon == null ? new Variables() : (AbstractMeta) spoon.getActiveMeta();
    this.internalShim = internalShim;
  }

  public JSONObject importNamedCluster( ThinNameClusterModel model,
                                        Map<String, CachedFileItemStream> siteFilesSource ) {
    JSONObject response = new JSONObject();
    response.put( NAMED_CLUSTER, "" );
    try {
      // Create and initialize template.
      NamedCluster nc = namedClusterService.getClusterTemplate();
      nc.setHdfsHost( "" );
      nc.setHdfsPort( "" );
      nc.setJobTrackerHost( "" );
      nc.setJobTrackerPort( "" );
      nc.setZooKeeperHost( "" );
      nc.setZooKeeperPort( "" );
      nc.setOozieUrl( "" );
      nc.setName( model.getName() );
      nc.setHdfsUsername( model.getHdfsUsername() );
      nc.setHdfsPassword( nc.encodePassword( model.getHdfsPassword() ) );
      if (MAPR_SHIM.equals(model.getShimVendor()))
        nc.setStorageScheme(MAPRFS_SCHEME);
      if ( variableSpace != null ) {
        nc.shareVariablesWith( variableSpace );
      } else {
        nc.initializeVariablesFrom( null );
      }

      boolean isConfigurationSet =
        configureNamedCluster( siteFilesSource, nc, model.getShimVendor(), model.getShimVersion() );
      if ( isConfigurationSet ) {
        deleteNamedClusterSchemaOnly( model );
        setupKnoxSecurity( nc, model );
        deleteConfigFolder( nc.getName() );
        installSiteFiles( siteFilesSource, nc );
        namedClusterService.create( nc, metaStore );
        createConfigProperties( nc );
        setupKerberosSecurity( model, siteFilesSource, "", "" );
        response.put( NAMED_CLUSTER, nc.getName() );
      }
    } catch ( Exception e ) {
      log.logError( e.getMessage() );
    }
    return response;
  }

  private void deleteNamedClusterSchemaOnly( ThinNameClusterModel model ) throws MetaStoreException {
    List<String> existingNcNames = namedClusterService.listNames( metaStore );
    for ( String existingNcName : existingNcNames ) {
      if ( existingNcName.equalsIgnoreCase( model.getName() ) ) {
        namedClusterService.delete( existingNcName, metaStore );
      }
    }
  }

  private NamedCluster convertToNamedCluster( ThinNameClusterModel model ) {

    NamedCluster nc = namedClusterService.getClusterTemplate();
    nc.setName( model.getName() );
    nc.setHdfsHost( model.getHdfsHost() );
    nc.setHdfsPort( model.getHdfsPort() );
    nc.setHdfsUsername( model.getHdfsUsername() );
    nc.setHdfsPassword( nc.encodePassword( model.getHdfsPassword() ) );
    nc.setJobTrackerHost( model.getJobTrackerHost() );
    nc.setJobTrackerPort( model.getJobTrackerPort() );
    nc.setZooKeeperHost( model.getZooKeeperHost() );
    nc.setZooKeeperPort( model.getZooKeeperPort() );
    nc.setOozieUrl( model.getOozieUrl() );
    nc.setKafkaBootstrapServers( model.getKafkaBootstrapServers() );
    resolveShimIdentifier( nc, model.getShimVendor(), model.getShimVersion() );
    if (MAPR_SHIM.equals(model.getShimVendor()))
      nc.setStorageScheme(MAPRFS_SCHEME);
    setupKnoxSecurity( nc, model );
    if ( variableSpace != null ) {
      nc.shareVariablesWith( variableSpace );
    } else {
      nc.initializeVariablesFrom( null );
    }
    return nc;
  }

  private void deleteConfigFolder( String configFolderName ) throws IOException {
    File configFolder = new File( getNamedClusterConfigsRootDir() );
    File[] files = configFolder.listFiles();
    if ( files != null ) {
      for ( File file : files ) {
        if ( file.isDirectory() && file.getName().equalsIgnoreCase( configFolderName ) ) {
          FileUtils.deleteDirectory( file );
          break;
        }
      }
    }
  }

  public JSONObject createNamedCluster( ThinNameClusterModel model,
                                        Map<String, CachedFileItemStream> siteFilesSource ) {
    JSONObject response = new JSONObject();
    response.put( NAMED_CLUSTER, "" );
    try {
      NamedCluster nc = convertToNamedCluster( model );
      deleteConfigFolder( nc.getName() );
      installSiteFiles( siteFilesSource, nc );
      namedClusterService.create( nc, metaStore );
      createConfigProperties( nc );
      setupKerberosSecurity( model, siteFilesSource, "", "" );
      response.put( NAMED_CLUSTER, nc.getName() );
    } catch ( Exception e ) {
      log.logError( e.getMessage() );
    }
    return response;
  }

  public JSONObject editNamedCluster( ThinNameClusterModel model, boolean isEditMode,
                                      Map<String, CachedFileItemStream> siteFilesSource ) {
    JSONObject response = new JSONObject();
    response.put( NAMED_CLUSTER, "" );
    try {
      final NamedCluster newNc = namedClusterService.getNamedClusterByName( model.getName(), metaStore );
      final NamedCluster oldNc = namedClusterService.getNamedClusterByName( model.getOldName(), metaStore );
      // Must get the current shim identifier before the creation of the Named Cluster xml schema for later comparison.
      String shimId = oldNc.getShimIdentifier();
      final List<NamedClusterSiteFile> existingSiteFiles = oldNc.getSiteFiles();

      NamedCluster nc = convertToNamedCluster( model );
      nc.setSiteFiles( getIntersectionSiteFiles( model, existingSiteFiles ) );
      installSiteFiles( siteFilesSource, nc );
      if ( newNc != null ) {
        namedClusterService.update( nc, metaStore ); //new cluster name exists
      } else {
        namedClusterService.create( nc, metaStore ); //new cluster does not exist.  Use creation logic
      }

      File oldConfigFolder = new File( getNamedClusterConfigsRootDir() + fileSeparator + model.getOldName() );
      File newConfigFolder = new File( getNamedClusterConfigsRootDir() + fileSeparator + nc.getName() );

      // Copy all files from the old config folder to the new config folder.
      if ( !oldConfigFolder.getName().equalsIgnoreCase( newConfigFolder.getName() ) ) {
        deleteConfigFolder( nc.getName() );
        FileUtils.copyDirectory( oldConfigFolder, newConfigFolder );
      } else {
        boolean success = oldConfigFolder.renameTo( newConfigFolder );
        if ( !success ) {
          log.logError( "Renaming Named Cluster configuration folder failed." );
        }
      }


      // If the user changed the shim, create a new config.properties file that corresponds to that shim
      // in the new config folder. Also save the keytab locations to set them again in the new config.properties
      // unless the kerberos subtype is Password.
      String keytabAuthenticationLocation = "";
      String keytabImpersonationLocation = "";
      String kerberosSubType = model.getKerberosSubType();
      if ( !kerberosSubType.equals( KERBEROS_SUBTYPE.PASSWORD.getValue() ) ) {
        String configFile =
          getNamedClusterConfigsRootDir() + fileSeparator + nc.getName() + fileSeparator + CONFIG_PROPERTIES;
        PropertiesConfiguration config = new PropertiesConfiguration( new File( configFile ) );
        keytabAuthenticationLocation = (String) config.getProperty( KEYTAB_AUTHENTICATION_LOCATION );
        keytabImpersonationLocation = (String) config.getProperty( KEYTAB_IMPERSONATION_LOCATION );
      }
      if ( nc.getShimIdentifier() != null && !nc.getShimIdentifier().equals( shimId ) ) {
        createConfigProperties( nc );
      }
      setupKerberosSecurity( model, siteFilesSource, keytabAuthenticationLocation, keytabImpersonationLocation );

      // Delete old config folder.
      if ( isEditMode && !oldConfigFolder.getName().equalsIgnoreCase( newConfigFolder.getName() ) ) {
        deleteNamedCluster( metaStore, model.getOldName(), false );
      }

      response.put( NAMED_CLUSTER, nc.getName() );
    } catch ( Exception e ) {
      log.logError( e.getMessage() );
    }
    return response;
  }

  public InputStream getSiteFileInputStream( String namedCluster, String siteFile ) {
    NamedCluster nc = namedClusterService.getNamedClusterByName( namedCluster, this.metaStore );
    return nc.getSiteFileInputStream( siteFile );
  }

  public ThinNameClusterModel getNamedCluster( String namedCluster ) {
    ThinNameClusterModel model = null;
    try {
      List<NamedCluster> namedClusters = namedClusterService.list( metaStore );
      for ( NamedCluster nc : namedClusters ) {
        if ( nc.getName().equalsIgnoreCase( namedCluster ) ) {
          model = new ThinNameClusterModel();
          model.setName( nc.getName() );
          model.setHdfsHost( nc.getHdfsHost() );
          model.setHdfsUsername( nc.getHdfsUsername() );
          model.setHdfsPassword( nc.decodePassword( nc.getHdfsPassword() ) );
          model.setHdfsPort( nc.getHdfsPort() );
          model.setJobTrackerHost( nc.getJobTrackerHost() );
          model.setJobTrackerPort( nc.getJobTrackerPort() );
          model.setKafkaBootstrapServers( nc.getKafkaBootstrapServers() );
          model.setOozieUrl( nc.getOozieUrl() );
          model.setZooKeeperPort( nc.getZooKeeperPort() );
          model.setZooKeeperHost( nc.getZooKeeperHost() );
          resolveShimVendorAndVersion( model, nc.getShimIdentifier() );
          model.setGatewayPassword( nc.decodePassword( nc.getGatewayPassword() ) );
          model.setGatewayUrl( nc.getGatewayUrl() );
          model.setGatewayUsername( nc.getGatewayUsername() );
          model.setSecurityType( SECURITY_TYPE.NONE.getValue() );
          if ( nc.isUseGateway() ) {
            model.setSecurityType( SECURITY_TYPE.KNOX.getValue() );
          } else {
            resolveKerberosSecurity( model, nc );
          }
          model.setSiteFiles( nc.getSiteFiles().stream()
            .map( sf -> new SimpleImmutableEntry<>( NAME_KEY, sf.getSiteFileName() ) )
            .collect( Collectors.toList() ) );
          break;
        }
      }
    } catch ( MetaStoreException e ) {
      log.logError( e.getMessage() );
    }
    return model;
  }

  private boolean configureNamedCluster( Map<String, CachedFileItemStream> siteFilesSource, NamedCluster nc,
                                         String shimVendor, String shimVersion ) {
    resolveShimIdentifier( nc, shimVendor, shimVersion );

    String oozieBaseUrl = "oozie.base.url";
    Map<String, String> properties = new HashMap();
    extractProperties( siteFilesSource, "core-site.xml", properties, new String[] { "fs.defaultFS" } );
    extractProperties( siteFilesSource, "yarn-site.xml", properties,
            new String[] { "yarn.resourcemanager.address", "yarn.resourcemanager.hostname" } );
    extractProperties( siteFilesSource, "hive-site.xml", properties,
            new String[] { "hive.zookeeper.quorum", "hive.zookeeper.client.port" } );
    extractProperties( siteFilesSource, "oozie-site.xml", properties, new String[] { oozieBaseUrl } );
    if ( properties.get( oozieBaseUrl ) == null ) {
      extractProperties( siteFilesSource, "oozie-default.xml", properties, new String[] { oozieBaseUrl } );
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
      nc.setHdfsPort( hdfsURL.getPort() != -1 ? hdfsURL.getPort() + "" : "" );
      isConfigurationSet = true;
    }

    /*
     * Address taken from
     * yarn.resourcemanager.address
     * in
     * yarn-site.xml
     *
     * If address not available
     * Hostname taken from
     * yarn.resourcemanager.hostname
     * in
     * yarn-site.xml
     * */
    String jobTrackerAddress = properties.get( "yarn.resourcemanager.address" );
    String jobTrackerHostname = properties.get( "yarn.resourcemanager.hostname" );
    if ( jobTrackerAddress != null ) {
      Map<String, String> hostAndPort = extractHostAndPort( jobTrackerAddress );
      nc.setJobTrackerHost( hostAndPort.get( "host" ) );
      nc.setJobTrackerPort( hostAndPort.get( "port" ) );
      isConfigurationSet = true;
    } else if ( jobTrackerHostname != null ) {
      nc.setJobTrackerHost( jobTrackerHostname );
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

    if ( zooKeeperAddress != null ) {
      List<String> addresses = Arrays.asList( zooKeeperAddress.split( "," ) );
      List<String> hostNames = addresses.stream().map( address ->
              extractHostAndPort( address ).get( "host" ) ).collect( Collectors.toList() );
      zooKeeperAddress = String.join( ",", hostNames );
    }

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

  private void extractProperties( Map<String, CachedFileItemStream> siteFilesSource, String fileName,
                                  Map<String, String> properties,
                                  String[] keys ) {
    CachedFileItemStream siteFile = siteFilesSource.get( fileName );

    if ( siteFile != null ) {
      Document document = parseSiteFileDocument( siteFile );
      if ( document != null ) {
        XPathFactory xpathFactory = XPathFactory.newInstance();
        XPath xpath = xpathFactory.newXPath();
        for ( String key : keys ) {
          try {
            XPathExpression expr =
              xpath.compile( "/configuration/property[name[starts-with(.,'" + key + "')]]/value/text()" );
            NodeList nodes = (NodeList) expr.evaluate( document, XPathConstants.NODESET );
            if ( nodes.getLength() > 0 ) {
              properties.put( key, nodes.item( 0 ).getNodeValue() );
            }
          } catch ( XPathExpressionException e ) {
            log.logMinimal( e.getMessage() );
          }
        }
      }
    }
  }

  public JSONObject installDriver( FileItem driver ) {
    boolean success = false;
    if ( driver != null ) {
      String destination = Const.getShimDriverDeploymentLocation();

      try ( final InputStream driverStream = driver.getInputStream() ) {
        FileUtils.copyInputStreamToFile( driverStream,
          new File( destination + fileSeparator + driver.getFieldName() ) );
        success = true;
      } catch ( IOException e ) {
        log.logError( e.getMessage() );
      }
    }
    JSONObject response = new JSONObject();
    response.put( INSTALLED, success );
    return response;
  }

  /**
   * Get Intersection of siteFiles
   *
   * @param model
   * @param existingSiteFiles
   * @return a list of siteFiles at the intersection of siteFiles in the model and the existingSiteFiles
   */
  private List<NamedClusterSiteFile> getIntersectionSiteFiles( ThinNameClusterModel model,
                                                               List<NamedClusterSiteFile> existingSiteFiles ) {
    List<String> newSiteFileNames =
      Optional.ofNullable( model.getSiteFiles() )
        .map( Collection::stream )
        .orElseGet( Stream::empty )
        .map( SimpleImmutableEntry::getValue )
        .collect( Collectors.toList() );

    return existingSiteFiles.stream()
      .filter( siteFile -> newSiteFileNames.contains( siteFile.getSiteFileName() ) )
      .collect( Collectors.toList() );
  }

  private void installSiteFiles( Map<String, CachedFileItemStream> siteFileSource, NamedCluster nc )
    throws IOException {
    for ( Map.Entry<String, CachedFileItemStream> siteFile : siteFileSource.entrySet() ) {
      String name = siteFile.getValue().getFieldName();
      if ( isValidConfigurationFile( name ) ) {
        if ( name.equals( KEYTAB_AUTH_FILE ) || name.equals( KEYTAB_IMPL_FILE ) || !name.endsWith( "-site.xml" ) ) {
          name = extractFileNameFromFullPath( siteFile.getValue().getName() );
          addFileToConfigFolder( siteFile.getValue().getCachedOutputStream(), name, nc );
        } else {
          addFileToNamedClusterSiteFiles( siteFile, name, nc );
        }
      }
    }
  }

  private void addFileToConfigFolder( ByteArrayOutputStream outputStream, String fileName, NamedCluster nc )
    throws IOException {
    File destination = new File(
      getNamedClusterConfigsRootDir() + fileSeparator + nc.getName() + fileSeparator + fileName );
    destination.getParentFile().mkdirs();
    try ( OutputStream fos = new FileOutputStream( destination ) ) {
      outputStream.writeTo( fos );
    }
  }

  private void addFileToNamedClusterSiteFiles( Map.Entry<String, CachedFileItemStream> cachedFileItemStreamMapEntry,
                                               String fileName, NamedCluster nc )
    throws IOException {
    InputStream inputStream = cachedFileItemStreamMapEntry.getValue().getCachedInputStream();
    InputStreamReader isReader = new InputStreamReader( inputStream );
    BufferedReader reader = new BufferedReader( isReader );
    StringBuilder sb = new StringBuilder();
    String str;
    while ( ( str = reader.readLine() ) != null ) {
      sb.append( str );
    }
    //skip placeholder site file contents because the site file content is in the NamedCluster already
    if ( !sb.toString().equals( PLACEHOLDER_VALUE ) ) {
      boolean nameExists = false;
      //replace the contents if the name exists
      for ( NamedClusterSiteFile siteFile : nc.getSiteFiles() ) {
        if ( siteFile.getSiteFileName().equals( fileName ) ) {
          siteFile.setSiteFileContents( sb.toString() );
          nameExists = true;
          break;
        }
      }
      //Add the file if the name didn't exist
      if ( !nameExists ) {
        nc.addSiteFile(
          new NamedClusterSiteFileImpl( fileName, cachedFileItemStreamMapEntry.getValue().getLastModified(),
            sb.toString() ) );
      }
    }
  }

  public boolean isValidConfigurationFile( String fileName ) {
    return fileName != null && ( fileName.endsWith( "-site.xml" ) || fileName.endsWith( "-default.xml" )
      || fileName.equals( CONFIG_PROPERTIES ) || fileName.equals( KEYTAB_AUTH_FILE )
      || fileName.equals( KEYTAB_IMPL_FILE ) || fileName.equals( "data" ) );
  }

  private Document parseSiteFileDocument( CachedFileItemStream file ) {
    Document document = null;
    try {
      document = XMLHandler.loadXMLFile( file.getCachedInputStream() );
    } catch ( KettleXMLException e ) {
      log.logMinimal( String.format( "Site file %s is not a well formed XML document", file.getName() ) );
    }
    return document;
  }

  private void createConfigProperties( NamedCluster namedCluster ) throws IOException {
    Path clusterConfigDirPath = Paths.get( getNamedClusterConfigsRootDir() + fileSeparator + namedCluster.getName() );
    Path
      configPropertiesPath =
      Paths.get(
        getNamedClusterConfigsRootDir() + fileSeparator + namedCluster.getName() + fileSeparator + CONFIG_PROPERTIES );
    Files.createDirectories( clusterConfigDirPath );
    String sampleConfigProperties = namedCluster.getShimIdentifier() + "sampleconfig.properties";
    InputStream
      inputStream =
      HadoopClusterDelegateImpl.class.getClassLoader().getResourceAsStream( sampleConfigProperties );
    if ( inputStream != null ) {
      Files.copy( inputStream, configPropertiesPath, StandardCopyOption.REPLACE_EXISTING );
    }
  }

  private void setupKerberosSecurity( ThinNameClusterModel model, Map<String, CachedFileItemStream> siteFilesSource,
                                      String keytabAuthenticationLocation, String keytabImpersonationLocation ) {
    Path
      configPropertiesPath =
      Paths
        .get( getNamedClusterConfigsRootDir() + fileSeparator + model.getName() + fileSeparator + CONFIG_PROPERTIES );

    String securityType = model.getSecurityType();
    if ( !StringUtil.isEmpty( securityType ) ) {
      resetKerberosSecurity( configPropertiesPath );
      if ( securityType.equals( SECURITY_TYPE.KERBEROS.getValue() ) ) {
        String kerberosSubType = model.getKerberosSubType();
        if ( kerberosSubType.equals( KERBEROS_SUBTYPE.PASSWORD.getValue() ) ) {
          setupKerberosPasswordSecurity( configPropertiesPath, model );
        }
        if ( kerberosSubType.equals( KERBEROS_SUBTYPE.KEYTAB.getValue() ) ) {
          setupKeytabSecurity( model, configPropertiesPath, siteFilesSource, keytabAuthenticationLocation,
            keytabImpersonationLocation );
        }
      }
    }
  }

  private void resetKerberosSecurity( Path configPropertiesPath ) {
    try {
      PropertiesConfiguration config = new PropertiesConfiguration( configPropertiesPath.toFile() );
      config.setProperty( KEYTAB_AUTHENTICATION_LOCATION, "" );
      config.setProperty( KEYTAB_IMPERSONATION_LOCATION, "" );
      config.setProperty( IMPERSONATION, IMPERSONATION_TYPE.DISABLED.getValue() );
      config.setProperty( KERBEROS_AUTHENTICATION_USERNAME, "" );
      config.setProperty( KERBEROS_AUTHENTICATION_PASS, "" );
      config.setProperty( KERBEROS_IMPERSONATION_USERNAME, "" );
      config.setProperty( KERBEROS_IMPERSONATION_PASS, "" );
      config.save();
    } catch ( ConfigurationException e ) {
      log.logMinimal( e.getMessage() );
    }
  }

  private void resolveKerberosSecurity( ThinNameClusterModel model, NamedCluster nc ) {
    try {
      String configFile =
        getNamedClusterConfigsRootDir() + fileSeparator + nc.getName() + fileSeparator + CONFIG_PROPERTIES;
      PropertiesConfiguration config = new PropertiesConfiguration( new File( configFile ) );
      model.setKerberosAuthenticationUsername( (String) config.getProperty( KERBEROS_AUTHENTICATION_USERNAME ) );
      model.setKerberosAuthenticationPassword(
        Encr.decryptPasswordOptionallyEncrypted( (String) config.getProperty( KERBEROS_AUTHENTICATION_PASS ) ) );
      model.setKerberosImpersonationUsername( (String) config.getProperty( KERBEROS_IMPERSONATION_USERNAME ) );
      String impersonationPasswordValue =
        Encr.decryptPasswordOptionallyEncrypted( (String) config.getProperty( KERBEROS_IMPERSONATION_PASS ) );
      if ( "Encrypted".equals( impersonationPasswordValue ) ) {
        impersonationPasswordValue = "";
      }
      model.setKerberosImpersonationPassword( impersonationPasswordValue );

      String keytabAuthenticationLocation = (String) config.getProperty( KEYTAB_AUTHENTICATION_LOCATION );
      String keytabImpersonationLocation = (String) config.getProperty( KEYTAB_IMPERSONATION_LOCATION );

      // Resolve the keytab auth and impl files if set to be displayed in the UI.
      if ( !StringUtil.isEmpty( keytabAuthenticationLocation ) ) {
        String keytabAuthFile =
          keytabAuthenticationLocation.substring( keytabAuthenticationLocation.lastIndexOf( fileSeparator ) + 1 );
        model.setKeytabAuthFile( keytabAuthFile );
      }
      if ( !StringUtil.isEmpty( keytabImpersonationLocation ) ) {
        String keytabImpFile =
          keytabImpersonationLocation.substring( keytabImpersonationLocation.lastIndexOf( fileSeparator ) + 1 );
        model.setKeytabImpFile( keytabImpFile );
      }

      // If Kerberos security properties are empty then security type is None else if at least one of them has a
      // value then the security type is Kerberos
      if ( StringUtil.isEmpty( keytabAuthenticationLocation )
        && StringUtil.isEmpty( keytabImpersonationLocation )
        && StringUtil.isEmpty( model.getKerberosAuthenticationPassword() )
        && StringUtil.isEmpty( model.getKerberosImpersonationPassword() ) ) {
        model.setSecurityType( SECURITY_TYPE.NONE.getValue() );
      } else {
        model.setSecurityType( SECURITY_TYPE.KERBEROS.getValue() );
      }

      // If kerberos keytab impersonation and kerberos keytab impersonation location are empty then kerberos sub type
      // is Password else is Keytab
      if ( StringUtil.isEmpty( keytabAuthenticationLocation )
        && StringUtil.isEmpty( keytabImpersonationLocation ) ) {
        model.setKerberosSubType( KERBEROS_SUBTYPE.PASSWORD.getValue() );
      } else {
        model.setKerberosSubType( KERBEROS_SUBTYPE.KEYTAB.getValue() );
      }
    } catch ( ConfigurationException e ) {
      log.logError( e.getMessage() );
    }
  }

  private void setupKerberosPasswordSecurity( Path configPropertiesPath, ThinNameClusterModel model ) {
    try {
      PropertiesConfiguration config = new PropertiesConfiguration( configPropertiesPath.toFile() );
      config.setProperty( KERBEROS_AUTHENTICATION_USERNAME, model.getKerberosAuthenticationUsername() );
      if ( !StringUtil.isEmpty( model.getKerberosAuthenticationPassword() ) ) {
        config.setProperty( KERBEROS_AUTHENTICATION_PASS,
          Encr.encryptPasswordIfNotUsingVariables( model.getKerberosAuthenticationPassword() ) );
      } else {
        config.setProperty( KERBEROS_AUTHENTICATION_PASS, "" );
      }
      config.setProperty( KERBEROS_IMPERSONATION_USERNAME, model.getKerberosImpersonationUsername() );
      if ( !StringUtil.isEmpty( model.getKerberosImpersonationPassword() ) ) {
        config.setProperty( KERBEROS_IMPERSONATION_PASS,
          Encr.encryptPasswordIfNotUsingVariables( model.getKerberosImpersonationPassword() ) );
      } else {
        config.setProperty( KERBEROS_IMPERSONATION_PASS, "" );
      }
      if ( ( !StringUtil.isEmpty( model.getKerberosImpersonationUsername() )
        && !StringUtil.isEmpty( model.getKerberosImpersonationPassword() ) )
        || ( !StringUtil.isEmpty( model.getKerberosAuthenticationUsername() )
        && !StringUtil.isEmpty( model.getKerberosAuthenticationPassword() ) ) ) {
        config.setProperty( IMPERSONATION, IMPERSONATION_TYPE.SIMPLE.getValue() );
      } else {
        config.setProperty( IMPERSONATION, IMPERSONATION_TYPE.DISABLED.getValue() );
      }
      config.save();
    } catch ( ConfigurationException e ) {
      log.logMinimal( e.getMessage() );
    }
  }

  private String extractFileNameFromFullPath( String fileName ) {
    /*
     * This method is necessary because a difference in upload functionality from Linux and Windows.
     * On Linux the file uploaded is provided with the name only.
     * On Windows the file uploaded is provided with the full path and we only need the name.
     * */
    int lastIndex = fileName.lastIndexOf( '/' ) != -1 ? fileName.lastIndexOf( '/' ) : fileName.lastIndexOf( '\\' );
    lastIndex = lastIndex == -1 ? 0 : lastIndex + 1;
    fileName = fileName.substring( lastIndex );
    return fileName;
  }

  private void setupKeytabSecurity( ThinNameClusterModel model, Path configPropertiesPath,
                                    Map<String, CachedFileItemStream> siteFilesSource,
                                    String keytabAuthenticationLocation, String keytabImpersonationLocation ) {
    String namedClusterName = model.getName();
    CachedFileItemStream keytabAuthFile = siteFilesSource.get( KEYTAB_AUTH_FILE );
    CachedFileItemStream keytabImpFile = siteFilesSource.get( KEYTAB_IMPL_FILE );

    String authenticationLocation = keytabAuthenticationLocation;
    String impersonationLocation = keytabImpersonationLocation;

    if ( keytabAuthFile != null ) {
      String name = extractFileNameFromFullPath( keytabAuthFile.getName() );
      authenticationLocation =
        getNamedClusterConfigsRootDir() + fileSeparator + namedClusterName + fileSeparator + name;
    }
    if ( keytabImpFile != null ) {
      String name = extractFileNameFromFullPath( keytabImpFile.getName() );
      impersonationLocation =
        getNamedClusterConfigsRootDir() + fileSeparator + namedClusterName + fileSeparator + name;
    }

    try {
      PropertiesConfiguration config = new PropertiesConfiguration( configPropertiesPath.toFile() );

      // Authentication
      config.setProperty( KERBEROS_AUTHENTICATION_USERNAME, model.getKerberosAuthenticationUsername() );
      if ( !StringUtil.isEmpty( authenticationLocation ) ) {
        config.setProperty( KEYTAB_AUTHENTICATION_LOCATION, authenticationLocation );
      }

      // Impersonation
      config.setProperty( KERBEROS_IMPERSONATION_USERNAME, model.getKerberosImpersonationUsername() );
      if ( keytabImpFile == null && StringUtil.isEmpty( model.getKeytabImpFile() ) ) {
        config.setProperty( KEYTAB_IMPERSONATION_LOCATION, "" );
      } else if ( !StringUtil.isEmpty( impersonationLocation ) ) {
        config.setProperty( KEYTAB_IMPERSONATION_LOCATION, impersonationLocation );
      }

      if ( !StringUtil.isEmpty( (String) config.getProperty( KEYTAB_AUTHENTICATION_LOCATION ) )
        || !StringUtil.isEmpty( (String) config.getProperty( KEYTAB_IMPERSONATION_LOCATION ) ) ) {
        config.setProperty( IMPERSONATION, IMPERSONATION_TYPE.SIMPLE.getValue() );
      } else {
        config.setProperty( IMPERSONATION, IMPERSONATION_TYPE.DISABLED.getValue() );
      }

      config.save();
    } catch ( ConfigurationException e ) {
      log.logMinimal( e.getMessage() );
    }
  }

  private void setupKnoxSecurity( NamedCluster nc, ThinNameClusterModel model ) {
    if ( model.getSecurityType() != null && model.getSecurityType().equals( SECURITY_TYPE.KNOX.getValue() ) ) {
      String userName = model.getGatewayUsername();
      String url = model.getGatewayUrl();
      String password = model.getGatewayPassword();
      nc.setGatewayPassword( nc.encodePassword( password ) );
      nc.setGatewayUrl( url );
      nc.setGatewayUsername( userName );
      nc.setUseGateway(
        !StringUtil.isEmpty( userName ) && !StringUtil.isEmpty( url ) && !StringUtil.isEmpty( password ) );
    }
  }

  /*
   * Extract Hostname and Port from a hostname/URL pattern
   */
  private Map<String, String> extractHostAndPort( String urlPattern ) {
    final String HTTP_PATTERN = "http://";
    if ( !urlPattern.startsWith( HTTP_PATTERN ) ) {
      urlPattern = HTTP_PATTERN + urlPattern;
    }
    URI parsedURI = URI.create( urlPattern );
    Map<String, String> map = new HashMap<>();
    map.put( "host", parsedURI.getHost() );
    map.put( "port", parsedURI.getPort() != -1 ? parsedURI.getPort() + "" : "" );
    return map;
  }

  public void deleteNamedCluster( IMetaStore metaStore, String namedCluster, boolean refreshTree ) {
    try {
      if ( namedClusterService.read( namedCluster, metaStore ) != null ) {
        namedClusterService.delete( namedCluster, metaStore );
        deleteConfigFolder( namedCluster );
      }
      if ( refreshTree ) {
        refreshTree();
      }
    } catch ( Exception e ) {
      log.logMinimal( e.getMessage() );
    }
  }

  /**
   * @return shim identifiers, excluding the internal shim, which should not be exposed to the cluster ui.
   */
  List<ShimIdentifierInterface> getShimIdentifiers() {
    List<ShimIdentifierInterface> shims = shimIdentifiersSupplier.get();
    shims.sort( Comparator.comparing( ShimIdentifierInterface::getVendor ) );
    return shims;
  }

  public NamedCluster getNamedClusterByName( String namedCluster) {
    return namedClusterService.getNamedClusterByName( namedCluster, this.metaStore );
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
        log.logMinimal( e.getLocalizedMessage() );
      }
      return produceTestCategories( runtimeTestStatus, nc );
    } else {
      return "[]";
    }
  }

  public Object[] produceTestCategories( RuntimeTestStatus runtimeTestStatus, NamedCluster nc ) {

    LinkedHashMap<String, TestCategory> categories = new LinkedHashMap<>();
    categories.put( HADOOP_FILE_SYSTEM, new TestCategory( "Hadoop file system" ) );
    categories.put( ZOOKEEPER, new TestCategory( "Zookeeper connection" ) );
    categories.put( MAP_REDUCE, new TestCategory( "Job tracker / resource manager" ) );
    categories.put( OOZIE, new TestCategory( "Oozie host connection" ) );
    categories.put( KAFKA, new TestCategory( "Kafka connection" ) );

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
            configureHadoopFileSystemTestCategory( category, !StringUtil.isEmpty( nc.getHdfsHost() ), status );
          } else if ( module.equals( OOZIE ) ) {
            configureTestCategories( category, !StringUtil.isEmpty( nc.getOozieUrl() ), status );
          } else if ( module.equals( KAFKA ) ) {
            configureTestCategories( category, !StringUtil.isEmpty( nc.getKafkaBootstrapServers() ), status );
          } else if ( module.equals( ZOOKEEPER ) ) {
            configureTestCategories( category, !StringUtil.isEmpty( nc.getZooKeeperHost() ), status );
          } else if ( module.equals( MAP_REDUCE ) ) {
            configureTestCategories( category, !StringUtil.isEmpty( nc.getJobTrackerHost() ), status );
          }
        }
      }
    }
    return categories.values().toArray();
  }

  private void configureHadoopFileSystemTestCategory( Category category, boolean isActive, String status ) {
    category.setCategoryActive( isActive );
    if ( category.isCategoryActive() ) {
      String currentStatus = category.getCategoryStatus();
      if ( status.equals( FAIL ) || ( status.equals( WARNING ) && !currentStatus.equals( FAIL ) ) || (
        status.equals( PASS ) && StringUtil.isEmpty( currentStatus ) ) ) {
        category.setCategoryStatus( status );
      }
    }
  }

  private void configureTestCategories( Category category, boolean isActive, String status ) {
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
      case WARNING:
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

  @VisibleForTesting
  void refreshTree() {
    if ( spoon != null && spoon.getShell() != null ) {
      spoon.getShell().getDisplay().asyncExec( () -> spoon.refreshTree( STRING_NAMED_CLUSTERS ) );
    }
  }

  @VisibleForTesting
  String getNamedClusterConfigsRootDir() {
    return System.getProperty( "user.home" ) + File.separator + ".pentaho" + File.separator + "metastore"
      + File.separator + "pentaho" + File.separator + "NamedCluster" + File.separator + "Configs";
  }
}
