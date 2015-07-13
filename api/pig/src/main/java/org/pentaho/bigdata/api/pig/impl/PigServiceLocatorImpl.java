package org.pentaho.bigdata.api.pig.impl;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.bigdata.api.pig.PigService;
import org.pentaho.bigdata.api.pig.PigServiceFactory;
import org.pentaho.bigdata.api.pig.PigServiceLocator;

import java.util.List;

/**
 * Created by bryan on 7/6/15.
 */
public class PigServiceLocatorImpl implements PigServiceLocator {
  private final List<PigServiceFactory> pigServiceFactories;

  public PigServiceLocatorImpl( List<PigServiceFactory> pigServiceFactories ) {
    this.pigServiceFactories = pigServiceFactories;
  }

  @Override public PigService getPigService( NamedCluster namedCluster ) {
    for ( PigServiceFactory pigServiceFactory : pigServiceFactories ) {
      if ( pigServiceFactory.canHandle( namedCluster ) ) {
        return pigServiceFactory.create( namedCluster );
      }
    }
    return null;
  }
}
