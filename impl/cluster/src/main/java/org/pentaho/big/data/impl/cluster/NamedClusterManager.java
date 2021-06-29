/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2020 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.impl.cluster;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FileUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.KettleClientEnvironment;
import org.pentaho.di.core.attributes.metastore.EmbeddedMetaStore;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.osgi.api.NamedClusterSiteFile;
import org.pentaho.di.core.osgi.impl.NamedClusterSiteFileImpl;
import org.pentaho.di.core.plugins.LifecyclePluginType;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.steps.named.cluster.NamedClusterEmbedManager;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.stores.xml.XmlMetaStore;
import org.pentaho.metastore.stores.xml.XmlUtil;
import org.pentaho.metastore.util.PentahoDefaults;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class NamedClusterManager implements NamedClusterService {

  public static final String BIG_DATA_SLAVE_METASTORE_DIR = "hadoop.configurations.path";
  private static final Class<?> PKG = NamedClusterManager.class;
  private BundleContext bundleContext;

  private Map<IMetaStore, MetaStoreFactory<NamedClusterImpl>> factoryMap = new HashMap<>();

  private NamedCluster clusterTemplate;

  private LogChannel log = new LogChannel( this );

  private Map<String, Object> properties = new HashMap<>();
  private static final String LOCALHOST = "localhost";
  private static final List<String> siteFileNames =
    Arrays.asList( "hdfs-site.xml", "core-site.xml", "mapred-site.xml", "yarn-site.xml",
      "hbase-site.xml", "hive-site.xml" );

  public BundleContext getBundleContext() {
    return bundleContext;
  }

  public void setBundleContext( BundleContext bundleContext ) {
    this.bundleContext = bundleContext;
    initProperties();
  }

  protected void initProperties() {
    final ServiceReference<?> serviceReference =
      getBundleContext().getServiceReference( ConfigurationAdmin.class.getName() );
    if ( serviceReference != null ) {
      try {
        final ConfigurationAdmin admin = (ConfigurationAdmin) getBundleContext().getService( serviceReference );
        final Configuration configuration = admin.getConfiguration( "pentaho.big.data.impl.cluster" );
        Dictionary<String, Object> rawProperties = configuration.getProperties();
        for ( Enumeration<String> keys = rawProperties.keys(); keys.hasMoreElements(); ) {
          String key = keys.nextElement();
          properties.put( key, rawProperties.get( key ) );
        }
      } catch ( Exception e ) {
        properties = new HashMap<>();
      }
    }
  }

  /**
   * returns a NamedClusterMetaStoreFactory for a given MetaStore instance. NOTE:  This method caches and returns a
   * factory for Embedded MetaStores.  For all other MetaStores, a new instance of MetaStoreFactory will always be
   * returned.
   *
   * @param metastore - the MetaStore for which to to get a MetaStoreFactory.
   * @return a MetaStoreFactory for the given MetaStore.
   */
  @VisibleForTesting
  MetaStoreFactory<NamedClusterImpl> getMetaStoreFactory( IMetaStore metastore ) {
    MetaStoreFactory<NamedClusterImpl> namedClusterMetaStoreFactory = null;

    // Only MetaStoreFactories for EmbeddedMetaStores are cached.  For all other MetaStore types, create a new
    // MetaStoreFactory
    if ( !( metastore instanceof EmbeddedMetaStore ) ) {
      return new MetaStoreFactory<>( NamedClusterImpl.class, metastore, PentahoDefaults.NAMESPACE );
    }

    // cache MetaStoreFactories for Embedded MetaStores
    namedClusterMetaStoreFactory = factoryMap.computeIfAbsent( metastore,
      m -> ( new MetaStoreFactory<>( NamedClusterImpl.class, m, NamedClusterEmbedManager.NAMESPACE ) ) );

    return namedClusterMetaStoreFactory;
  }

  @VisibleForTesting
  void putMetaStoreFactory( IMetaStore metastore, MetaStoreFactory<NamedClusterImpl> metaStoreFactory ) {
    factoryMap.put( metastore, metaStoreFactory );
  }

  @Override public void close( IMetaStore metastore ) {
    factoryMap.remove( metastore );
  }

  @Override
  public NamedCluster getClusterTemplate() {
    if ( clusterTemplate == null ) {
      clusterTemplate = new NamedClusterImpl();
      clusterTemplate.setName( "" );
      clusterTemplate.setHdfsHost( LOCALHOST );
      clusterTemplate.setHdfsPort( "8020" );
      clusterTemplate.setHdfsUsername( "user" );
      clusterTemplate.setHdfsPassword( clusterTemplate.encodePassword( "password" ) );
      clusterTemplate.setJobTrackerHost( LOCALHOST );
      clusterTemplate.setJobTrackerPort( "8032" );
      clusterTemplate.setZooKeeperHost( LOCALHOST );
      clusterTemplate.setZooKeeperPort( "2181" );
      clusterTemplate.setOozieUrl( "http://localhost:8080/oozie" );
    }
    return clusterTemplate.clone();
  }

  @Override
  public void setClusterTemplate( NamedCluster clusterTemplate ) {
    this.clusterTemplate = clusterTemplate;
  }

  @Override
  public void create( NamedCluster namedCluster, IMetaStore metastore ) throws MetaStoreException {
    getMetaStoreFactory( metastore ).saveElement( new NamedClusterImpl( namedCluster ) );
  }

  @Override
  public NamedCluster read( String clusterName, IMetaStore metastore ) throws MetaStoreException {
    MetaStoreFactory<NamedClusterImpl> factory = getMetaStoreFactory( metastore );

    if ( metastore == null || !listNames( metastore ).contains( clusterName ) ) {
      // only try the slave metastore if the given one fails
      IMetaStore slaveMetastore = getSlaveServerMetastore();
      if ( slaveMetastore != null && listNames( slaveMetastore ).contains( clusterName ) ) {
        factory = getMetaStoreFactory( slaveMetastore );
      }
    }

    NamedCluster namedCluster = null;
    try {
      namedCluster = factory.loadElement( clusterName );
    } catch ( MetaStoreException e ) {
      // While executing Pentaho MapReduce on a secure cluster, the .lock file
      // might not be able to be created due to permissions.
      // In this case, try and read the MetaStore without locking.
      namedCluster = factory.loadElement( clusterName, false );
    }
    return namedCluster;
  }

  @Override
  public void update( NamedCluster namedCluster, IMetaStore metastore ) throws MetaStoreException {
    MetaStoreFactory<NamedClusterImpl> factory = getMetaStoreFactory( metastore );
    List<NamedCluster> namedClusters = list( metastore );
    for ( NamedCluster nc : namedClusters ) {
      if ( namedCluster.getName().equals( nc.getName() ) ) {
        factory.deleteElement( nc.getName() );
        factory.saveElement( new NamedClusterImpl( namedCluster ) );
      }
    }
  }

  @Override
  public void delete( String clusterName, IMetaStore metastore ) throws MetaStoreException {
    getMetaStoreFactory( metastore ).deleteElement( clusterName );
  }

  @Override
  public List<NamedCluster> list( IMetaStore metastore ) throws MetaStoreException {
    MetaStoreFactory<NamedClusterImpl> factory = getMetaStoreFactory( metastore );
    List<NamedCluster> namedClusters;
    List<MetaStoreException> exceptionList = new ArrayList<>();

    try {
      namedClusters = new ArrayList<>( factory.getElements( true, exceptionList ) );
    } catch ( MetaStoreException ex ) {
      // While executing Pentaho MapReduce on a secure cluster, the .lock file
      // might not be able to be created due to permissions.
      // In this case, try and read the MetaStore without locking.
      namedClusters = new ArrayList<>( factory.getElements( false, exceptionList ) );
    }

    return namedClusters;
  }

  /**
   * This method lists the NamedClusters in the given IMetaStore.  If an exception is thrown when parsing the data for a
   * given NamedCluster.  The exception will be added to the exceptionList, but list generation will continue.
   *
   * @param metastore     the IMetaStore to operate with
   * @param exceptionList As list to hold any exceptions that occur
   * @return the list of NamedClusters in the provided IMetaStore
   * @throws MetaStoreException
   */
  @Override
  public List<NamedCluster> list( IMetaStore metastore, List<MetaStoreException> exceptionList )
    throws MetaStoreException {
    MetaStoreFactory<NamedClusterImpl> factory = getMetaStoreFactory( metastore );
    return new ArrayList<>( factory.getElements( false, exceptionList ) );
  }

  @Override
  public List<String> listNames( IMetaStore metastore ) throws MetaStoreException {
    return getMetaStoreFactory( metastore ).getElementNames( false );
  }

  @Override
  public boolean contains( String clusterName, IMetaStore metastore ) throws MetaStoreException {
    boolean found = false;
    if ( metastore != null ) {
      found = listNames( metastore ).contains( clusterName );
    }
    if ( !found ) {
      IMetaStore slaveMetastore = getSlaveServerMetastore();
      if ( slaveMetastore != null ) {
        found = listNames( slaveMetastore ).contains( clusterName );
      }
    }
    return found;
  }

  @Override
  public NamedCluster getNamedClusterByName( String namedClusterName, IMetaStore metastore ) {
    NamedCluster namedCluster = null;
    if ( metastore != null ) {
      namedCluster = searchMetastoreByName( namedClusterName, metastore );
    }
    if ( namedCluster == null ) {
      IMetaStore slaveMetastore = getSlaveServerMetastore();
      if ( slaveMetastore != null ) {
        namedCluster = searchMetastoreByName( namedClusterName, slaveMetastore );
      }
    }
    loadSiteFilesIfNecessary( namedCluster, metastore );
    return namedCluster;
  }

  private NamedCluster searchMetastoreByName( String namedCluster, IMetaStore metastore ) {
    try {
      List<NamedCluster> namedClusters = list( metastore );
      for ( NamedCluster nc : namedClusters ) {
        if ( nc.getName().equals( namedCluster ) ) {
          return nc;
        }
      }
    } catch ( MetaStoreException e ) {
      return null;
    }
    return null;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  @Override
  public NamedCluster getNamedClusterByHost( String hostName, IMetaStore metastore ) {
    NamedCluster namedCluster = null;
    if ( hostName == null ) {
      return null;
    }
    if ( metastore != null ) {
      namedCluster = searchMetastoreByHost( hostName, metastore );
    }
    if ( namedCluster == null ) {
      IMetaStore slaveMetastore = getSlaveServerMetastore();
      if ( slaveMetastore != null ) {
        namedCluster = searchMetastoreByHost( hostName, slaveMetastore );
      }
    }
    return namedCluster;
  }

  private NamedCluster searchMetastoreByHost( String hostName, IMetaStore metastore ) {
    try {
      List<NamedCluster> namedClusters = list( metastore );
      for ( NamedCluster nc : namedClusters ) {
        if ( hostName.equals( nc.getHdfsHost() ) ) {
          loadSiteFilesIfNecessary( nc, metastore );
          return nc;
        }
      }
    } catch ( MetaStoreException e ) {
      return null;
    }
    return null;
  }

  @Override
  public void updateNamedClusterTemplate( String hostName, int port, boolean isMapr ) {
    if ( clusterTemplate == null ) {
      getClusterTemplate();
    }
    clusterTemplate.setHdfsHost( hostName );
    if ( port > 0 ) {
      clusterTemplate.setHdfsPort( String.valueOf( port ) );
    } else {
      clusterTemplate.setHdfsPort( "" );
    }
    clusterTemplate.setMapr( isMapr );
  }

  private String getSlaveServerMetastoreDir() throws IOException {
    PluginInterface pluginInterface =
      PluginRegistry.getInstance().findPluginWithId( LifecyclePluginType.class, "HadoopSpoonPlugin" );
    Properties legacyProperties;

    try {
      legacyProperties = loadProperties( pluginInterface, "plugin.properties" );
      String slaveMetaStorePath = legacyProperties.getProperty( BIG_DATA_SLAVE_METASTORE_DIR );
      FileObject slaveMetastoreDir;

      // check for user-specified metastore directory
      if ( useSlaveMetastorePathFromProperties( slaveMetaStorePath ) ) {
        return slaveMetaStorePath;
      }

      // see if metastore was copied to the big data plugin folder (yarn kettle cluster job)
      slaveMetaStorePath = pluginInterface.getPluginDirectory().getPath();
      slaveMetastoreDir =
        KettleVFS.getFileObject( slaveMetaStorePath + File.separator + XmlUtil.META_FOLDER_NAME );
      if ( null != slaveMetastoreDir && slaveMetastoreDir.exists()
        && slaveMetastoreDir.getType().equals( FileType.FOLDER )
        // last condition exists to ensure that this path doesn't get used if two jobs are running on a slave instance
        // at once, and one of them is packaging up the install for a yarn carte job
        && KettleClientEnvironment.getInstance().getClient().equals( KettleClientEnvironment.ClientType.CARTE ) ) {
        return slaveMetaStorePath;
      }

      slaveMetaStorePath = System.getProperty( "user.home" ) + File.separator + ".pentaho";
      slaveMetastoreDir =
        KettleVFS.getFileObject( slaveMetaStorePath );
      if ( null != slaveMetastoreDir && slaveMetastoreDir.exists()
        && slaveMetastoreDir.getType().equals( FileType.FOLDER ) ) {
        return slaveMetaStorePath;
      } else {
        return null;
      }
    } catch ( KettleFileException | NullPointerException e ) {
      log.logError( BaseMessages.getString( PKG, "NamedClusterManager.ErrorFindingUserMetastore" ), e );
      throw new IOException( e );
    }
  }

  private boolean useSlaveMetastorePathFromProperties( String slaveMetaStorePath ) throws FileSystemException {
    FileObject slaveMetastoreDir;
    try {
      slaveMetastoreDir = KettleVFS.getFileObject( slaveMetaStorePath + File.separator + XmlUtil.META_FOLDER_NAME );
      return null != slaveMetaStorePath && !slaveMetaStorePath.equals( "" )
        && null != slaveMetastoreDir && slaveMetastoreDir.exists();
    } catch ( KettleFileException e ) {
      log.logError( BaseMessages.getString( PKG, "NamedClusterManager.ErrorFindingUserMetastore" ), e );
    }
    return false;
  }

  @VisibleForTesting
  IMetaStore getSlaveServerMetastore() {
    try {
      String metastoreDir = getSlaveServerMetastoreDir();
      if ( null != metastoreDir ) {
        return new XmlMetaStore( getSlaveServerMetastoreDir() );
      } else {
        // it is essential that this method returns a null value if no slave metastore directory exists
        return null;
      }
    } catch ( IOException | MetaStoreException e ) {
      log.logError( BaseMessages.getString( PKG, "NamedClusterManager.ErrorReadingMetastore" ), e );
      return null;
    }
  }

  /**
   * Loads a properties file from the plugin directory for the plugin interface provided
   *
   * @param plugin
   * @return
   * @throws KettleFileException
   * @throws IOException
   */
  private Properties loadProperties( PluginInterface plugin, String relativeName ) throws KettleFileException,
    IOException {
    if ( plugin == null ) {
      throw new NullPointerException();
    }
    FileObject propFile =
      KettleVFS.getFileObject( plugin.getPluginDirectory().getPath() + Const.FILE_SEPARATOR + relativeName );
    if ( !propFile.exists() ) {
      throw new FileNotFoundException( propFile.toString() );
    }
    try {
      Properties pluginProperties = new Properties();
      pluginProperties.load( new FileInputStream( propFile.getName().getPath() ) );
      return pluginProperties;
    } catch ( Exception e ) {
      // Do not catch ConfigurationException. Different shims will use different
      // packages for this exception.
      throw new IOException( e );
    }
  }

  private void loadSiteFilesIfNecessary( NamedCluster namedCluster, IMetaStore metaStore ) {
    if ( namedCluster == null ) {
      return; //Can't do anything without a cluster
    }
    if ( namedCluster.getSiteFiles().isEmpty() ) {
      // This seeds the site files once if not already present - standard behavior
      unconditionalAddOfSiteFiles( namedCluster, metaStore );
      return;
    }
    if ( Boolean.parseBoolean( System.getProperties().getProperty( Const.KETTLE_AUTO_UPDATE_SITE_FILE ) ) ) {
      // Special mode that tries to update site files by checking modification time of the file against what
      // is stored in the named cluster
      semiIntelligentSiteFileUpdate( namedCluster, metaStore );
    }
  }

  private void unconditionalAddOfSiteFiles( NamedCluster namedCluster, IMetaStore metaStore ) {
    String rootDir = getNamedClusterConfigsRootDir( metaStore );
    for ( String siteFileName : siteFileNames ) {
      String path = rootDir + File.separator + namedCluster.getName() + File.separator + siteFileName;
      File file = new File( path );
      if ( file.exists() ) {
        try {
          namedCluster.addSiteFile( new NamedClusterSiteFileImpl( siteFileName, file.lastModified(),
            FileUtils.readFileToString( file, StandardCharsets.UTF_8.toString() ) ) );
        } catch ( IOException e ) {
          log.logError( "An error occurred importing " + path + " into HadoopCluster " + namedCluster.getName(), e );
        }
      }
    }
    if ( !namedCluster.getSiteFiles().isEmpty() ) {
      autoUpdateMetastoreWithSiteFiles( namedCluster, metaStore );
    }
  }

  private void semiIntelligentSiteFileUpdate( NamedCluster namedCluster, IMetaStore metaStore ) {
    String rootDir = getNamedClusterConfigsRootDir( metaStore );
    Map<String, NamedClusterSiteFile> map = namedCluster.getSiteFiles().stream().collect(
      Collectors.toMap( NamedClusterSiteFile::getSiteFileName, namedClusterSiteFile -> namedClusterSiteFile ) );
    List<NamedClusterSiteFile> newSiteFiles = new ArrayList<>();
    List<String> missingFiles = new ArrayList<>();
    for ( String siteFileName : siteFileNames ) {
      String path = rootDir + File.separator + namedCluster.getName() + File.separator + siteFileName;
      File file = new File( path );
      if ( file.exists() && ( map.get( siteFileName ) == null || file.lastModified() != map.get( siteFileName )
        .getSourceFileModificationTime() ) ) {
        try {
          newSiteFiles.add( new NamedClusterSiteFileImpl( siteFileName, file.lastModified(),
            FileUtils.readFileToString( file, StandardCharsets.UTF_8.toString() ) ) );
        } catch ( IOException e ) {
          log.logError( "An error occurred importing " + path + " into HadoopCluster " + namedCluster.getName(), e );
        }
      } else {
        //List of files where we need to retain the old site file if it exists
        missingFiles.add( siteFileName );
      }
    }
    // If there is nothing new then we don't need to change anything
    if ( !newSiteFiles.isEmpty() ) {
      //Bring in the old files not present
      for ( String siteFile : missingFiles ) {
        if ( map.get( siteFile ) != null ) {
          newSiteFiles.add( map.get( siteFile ) );
        }
      }
      //newSiteFiles is complete, update the named cluster and write the metastore entry
      namedCluster.setSiteFiles( newSiteFiles );
      autoUpdateMetastoreWithSiteFiles( namedCluster, metaStore );
    }
  }

  private void autoUpdateMetastoreWithSiteFiles( NamedCluster namedCluster, IMetaStore metaStore ) {
    boolean recoverOriginal = false;
    try {
      update( namedCluster, metaStore );
    } catch ( MetaStoreException e ) {
      log.logError( "An error occurred trying to save HadoopCluster " + namedCluster.getName()
        + " with embedded site files in the metastore.  Recovering original HadoopCluster.", e );
      recoverOriginal = true;
    }
    //As a safeguard make sure we can read the metastore
    if ( !recoverOriginal ) {
      try {
        getNamedClusterByName( namedCluster.getName(), metaStore );
      } catch ( Exception e ) {
        log.logError( "Could not successfully read back Hadoop Cluster " + namedCluster.getName()
          + " after embedding site files.  Recovering original HadoopCluster." );
        recoverOriginal = true;
      }
    }
    if ( recoverOriginal ) {
      // We can't read the metastore or could store the new one.  Try to put the old hadoop cluster back
      namedCluster.setSiteFiles( new ArrayList<NamedClusterSiteFile>() );
      try {
        update( namedCluster, metaStore );
      } catch ( MetaStoreException e ) {
        log.logError( "An error occurred trying to recover the old HadoopCluster" + namedCluster.getName(), e );
      }
    }
  }

  private String getNamedClusterConfigsRootDir( IMetaStore metaStore ) {
    String rootDir = metaStore instanceof XmlMetaStore ? ( (XmlMetaStore) metaStore ).getRootFolder()
      : System.getProperty( "user.home" ) + File.separator + ".pentaho" + File.separator + "metastore";

    return rootDir + File.separator + "pentaho" + File.separator + "NamedCluster" + File.separator + "Configs";
  }
}
