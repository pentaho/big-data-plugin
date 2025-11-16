/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package org.pentaho.big.data.services.bootstrap;

import org.apache.logging.log4j.Logger;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.service.PluginServiceLoader;
import org.pentaho.di.core.annotations.KettleLifecyclePlugin;
import org.pentaho.di.core.lifecycle.KettleLifecycleListener;
import org.pentaho.hadoop.shim.api.services.BigDataServicesInitializer;

import java.util.Collection;


@KettleLifecyclePlugin( id = "BigDataPlugin", name = "Big Data Plugin" )
public class BigDataPluginLifecycleListener implements KettleLifecycleListener {

  protected static final Logger logger = BigDataLogConfig.getBigDataLogger(BigDataPluginLifecycleListener.class);




  @Override
  public void onEnvironmentInit() {
    try {
      Collection<BigDataServicesInitializer> bigDataServicesInitializerCollection = PluginServiceLoader.loadServices( BigDataServicesInitializer.class );
      BigDataServicesInitializer bigDataServicesInitializer = bigDataServicesInitializerCollection.stream().findFirst().get();
      bigDataServicesInitializer.doInitialize();
    } catch ( KettlePluginException e ) {
      throw new RuntimeException( e );
    }
  }


  @Override
  public void onEnvironmentShutdown() {
    // No action needed on exit
  }
}
