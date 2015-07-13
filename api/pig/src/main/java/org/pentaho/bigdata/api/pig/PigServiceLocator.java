package org.pentaho.bigdata.api.pig;

import org.pentaho.big.data.api.cluster.NamedCluster;

/**
 * Created by bryan on 7/6/15.
 */
public interface PigServiceLocator {
  PigService getPigService( NamedCluster namedCluster );
}
