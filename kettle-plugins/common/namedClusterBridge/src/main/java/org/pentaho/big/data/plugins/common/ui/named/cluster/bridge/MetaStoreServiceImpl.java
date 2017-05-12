package org.pentaho.big.data.plugins.common.ui.named.cluster.bridge;

import org.pentaho.big.data.api.cluster.MetaStoreService;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.stores.xml.XmlMetaStore;
import org.pentaho.metastore.stores.xml.XmlUtil;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Properties;

/**
 * Created by dstepanov on 11/05/17.
 */
public class MetaStoreServiceImpl implements MetaStoreService {

  private IMetaStore metaStore;

  public MetaStoreServiceImpl() {
  }

  private IMetaStore getSpoonMetaStore() {
    try {
      Class spoonClass = this.getClass().getClassLoader().loadClass( "org.pentaho.di.ui.spoon.Spoon" );
      Method getSpoonInstance = spoonClass.getDeclaredMethod( "getInstance" );
      Method getSpoonMetastore = spoonClass.getDeclaredMethod( "getMetaStore" );
      Object spoon = getSpoonInstance.invoke( null );
      return (IMetaStore) getSpoonMetastore.invoke( spoonClass.cast( spoon ) );
    } catch ( ClassNotFoundException e ) {
      e.printStackTrace();
    } catch ( NoSuchMethodException e ) {
      e.printStackTrace();
    } catch ( IllegalAccessException e ) {
      e.printStackTrace();
    } catch ( InvocationTargetException e ) {
      e.printStackTrace();
    } catch ( Throwable e ) {
      e.printStackTrace();
    }
    return null;
  }

  @Override public IMetaStore getMetaStore() {
    if ( metaStore == null ) {
      if ( !isPmr() ) {
        metaStore = getSpoonMetaStore();
      }
      if ( metaStore == null ) {
        metaStore = getPMRMetaStore();
      }
    }
    return metaStore;
  }

  private boolean isPmr() {
    InputStream pmrProperties = HadoopConfigurationBootstrap.class.getClassLoader().getResourceAsStream(
      "pmr.properties" );
    if ( pmrProperties != null ) {
      Properties properties = new Properties();
      try {
        properties.load( pmrProperties );
        String isPmr = properties.getProperty( "isPmr", "false" );
        return "true".equals( isPmr );

      } catch ( IOException ioe ) {
        // pmr.properties not available
      }
    }
    return false;
  }

  private IMetaStore getPMRMetaStore() {
    String dir = System.getProperty( Const.PENTAHO_METASTORE_FOLDER );
    try {
      return MetaStoreServiceImpl.createMetaStore( dir );
    } catch ( MetaStoreException e ) {
      e.printStackTrace();
      return null;
    }
  }


  public static IMetaStore createMetaStore( String rootFolder ) throws MetaStoreException {
    File rootFolderFile = new File( rootFolder ); //TODO move to kettle core?
    File metaFolder = new File( rootFolder + File.separator + XmlUtil.META_FOLDER_NAME );
    if ( !metaFolder.exists() ) {
      return null;
    }
    if ( !rootFolderFile.exists() ) {
      rootFolderFile.mkdirs();
    }

    XmlMetaStore metaStore = new XmlMetaStore( rootFolder );
    metaStore.setName( Const.PENTAHO_METASTORE_NAME );

    return metaStore;
  }
}
