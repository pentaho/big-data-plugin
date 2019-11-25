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

package org.pentaho.big.data.impl.cluster;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.attributes.metastore.EmbeddedMetaStore;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.logging.LogChannel;
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
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class NamedClusterManager implements NamedClusterService {

  public static final String BIG_DATA_SLAVE_METASTORE_DIR = "hadoop.configurations.path";
  private static final Class<?> PKG = NamedClusterManager.class;
  private BundleContext bundleContext;

  private Map<IMetaStore, MetaStoreFactory<NamedClusterImpl>> factoryMap = new HashMap<>();

  private NamedCluster clusterTemplate;

  private LogChannel log = new LogChannel( this );

  private Map<String, Object> properties = new HashMap<>();
  private static final String LOCALHOST = "localhost";

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
      clusterTemplate.setHdfsPassword( "password" );
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

    try {
      namedClusters = new ArrayList<>( factory.getElements( true ) );
    } catch ( MetaStoreException ex ) {
      // While executing Pentaho MapReduce on a secure cluster, the .lock file
      // might not be able to be created due to permissions.
      // In this case, try and read the MetaStore without locking.
      namedClusters = new ArrayList<>( factory.getElements( false ) );
    }

    return namedClusters;
  }

  @Override
  public List<String> listNames( IMetaStore metastore ) throws MetaStoreException {
    return getMetaStoreFactory( metastore ).getElementNames();
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
}
