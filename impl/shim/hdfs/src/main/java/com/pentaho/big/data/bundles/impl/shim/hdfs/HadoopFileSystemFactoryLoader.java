package com.pentaho.big.data.bundles.impl.shim.hdfs;

import com.pentaho.big.data.bundles.impl.shim.common.ShimBridgingServiceTracker;
import org.osgi.framework.BundleContext;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemFactory;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.di.core.hadoop.HadoopConfigurationListener;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by bryan on 6/4/15.
 */
public class HadoopFileSystemFactoryLoader implements HadoopConfigurationListener {
  public static final String HADOOP_FILESYSTEM_FACTORY_IMPL_CANONICAL_NAME =
    HadoopFileSystemFactoryImpl.class.getCanonicalName();
  private static final Logger LOGGER = LoggerFactory.getLogger( HadoopFileSystemFactoryLoader.class );
  private final BundleContext bundleContext;
  private final ShimBridgingServiceTracker shimBridgingServiceTracker;

  public HadoopFileSystemFactoryLoader( BundleContext bundleContext,
                                        ShimBridgingServiceTracker shimBridgingServiceTracker )
    throws ConfigurationException {
    this.bundleContext = bundleContext;
    this.shimBridgingServiceTracker = shimBridgingServiceTracker;
    HadoopConfigurationBootstrap.getInstance().registerHadoopConfigurationListener( this );
  }

  @Override public void onConfigurationOpen( HadoopConfiguration hadoopConfiguration, boolean defaultConfiguration ) {
    try {
      shimBridgingServiceTracker.registerWithClassloader( hadoopConfiguration, HadoopFileSystemFactory.class,
        HADOOP_FILESYSTEM_FACTORY_IMPL_CANONICAL_NAME,
        bundleContext, hadoopConfiguration.getHadoopShim().getClass().getClassLoader(),
        new Class<?>[] { boolean.class, HadoopConfiguration.class },
        new Object[] { defaultConfiguration, hadoopConfiguration } );
    } catch ( Exception e ) {
      LOGGER.error( "Unable to register " + hadoopConfiguration.getIdentifier() + " shim", e );
    }
  }

  @Override public void onConfigurationClose( HadoopConfiguration hadoopConfiguration ) {
    shimBridgingServiceTracker.unregister( hadoopConfiguration );
  }
}
