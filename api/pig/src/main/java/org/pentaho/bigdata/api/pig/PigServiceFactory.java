package org.pentaho.bigdata.api.pig;

import org.pentaho.big.data.api.cluster.NamedCluster;

/**
 * Created by bryan on 6/18/15.
 */
public interface PigServiceFactory {
  boolean canHandle( NamedCluster namedCluster );
  PigService create( NamedCluster namedCluster );
}
