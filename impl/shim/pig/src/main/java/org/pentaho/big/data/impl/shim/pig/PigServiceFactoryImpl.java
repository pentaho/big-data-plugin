package org.pentaho.big.data.impl.shim.pig;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.bigdata.api.pig.PigService;
import org.pentaho.bigdata.api.pig.PigServiceFactory;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.hadoop.shim.spi.PigShim;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by bryan on 7/6/15.
 */
public class PigServiceFactoryImpl implements PigServiceFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger( PigServiceFactoryImpl.class );
  private final boolean isActiveConfiguration;
  private final HadoopConfiguration hadoopConfiguration;

  public PigServiceFactoryImpl( boolean isActiveConfiguration,
                                HadoopConfiguration hadoopConfiguration ) {
    this.isActiveConfiguration = isActiveConfiguration;
    this.hadoopConfiguration = hadoopConfiguration;
  }

  @Override public boolean canHandle( NamedCluster namedCluster ) {
    String shimIdentifier = null; // TODO: Specify shim
    return ( shimIdentifier == null && isActiveConfiguration ) || hadoopConfiguration.getIdentifier()
      .equals( shimIdentifier );
  }

  @Override public PigService create( NamedCluster namedCluster ) {
    try {
      PigShim pigShim = hadoopConfiguration.getPigShim();
      HadoopShim hadoopShim = hadoopConfiguration.getHadoopShim();
      return new PigServiceImpl( namedCluster, pigShim, hadoopShim );
    } catch ( ConfigurationException e ) {
      LOGGER.error( "Unable to create PigService for " + namedCluster, e );
    }
    return null;
  }
}
