/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

package org.pentaho.big.data.bundles.impl.shim.mapr.jaas;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import javax.security.auth.login.AppConfigurationEntry;

import com.google.common.annotations.VisibleForTesting;
import org.apache.karaf.jaas.config.JaasRealm;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.di.core.hadoop.HadoopConfigurationListener;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.security.auth.login.ConfigFile;

/**
 * Class used to append jaas configuration set via <code>java.security.auth.login.config</code> system property to
 * currently active jaas configuration.
 */
public class MaprJaasRealmsRegistrar implements HadoopConfigurationListener {
  private static final Logger LOGGER = LoggerFactory.getLogger( MaprJaasRealmsRegistrar.class );
  private BundleContext bundleContext;
  private List<ServiceRegistration> realmRegistrations;

  public MaprJaasRealmsRegistrar( BundleContext bundleContext ) throws ConfigurationException {
    this.bundleContext = bundleContext;
    HadoopConfigurationBootstrap.getInstance().registerHadoopConfigurationListener( this );
  }

  public void onConfigurationOpen( HadoopConfiguration hadoopConfiguration, boolean defaultConfiguration ) {
    if ( !isMaprShimActive( hadoopConfiguration ) ) {
      LOGGER.info( "Active hadoop configuration is not MapR. Skipping JAAS realms registration." );
      return;
    }

    try {
      HashMap<String, LinkedList<AppConfigurationEntry>> configs = getMaprJaasConfig();

      realmRegistrations = new ArrayList<>( configs.size() );

      for ( final String realmName : configs.keySet() ) {
        JaasRealm realm = createJaasRealm( realmName, configs.get( realmName ) );
        ServiceRegistration reg = getBundleContext().registerService( JaasRealm.class.getCanonicalName(), realm, null );
        realmRegistrations.add( reg );
      }

      LOGGER.debug( String.format( "Registered %s JAAS realms using system properties.", realmRegistrations.size() ) );
    } catch ( Exception e ) {
      LOGGER.error( "Error during setting up MapR JAAS configuration", e );
    }
  }

  public void onConfigurationClose( HadoopConfiguration hadoopConfiguration ) {
    if ( realmRegistrations != null ) {
      for ( ServiceRegistration realmRegistration : realmRegistrations ) {
        realmRegistration.unregister();
      }

      realmRegistrations = null;
    }
  }

  BundleContext getBundleContext() {
    return bundleContext;
  }

  private HashMap<String, LinkedList<AppConfigurationEntry>> getMaprJaasConfig() throws Exception {
    try {
      ConfigFile config = new ConfigFile();
      Field f = config.getClass().getDeclaredField( "configuration" );

      f.setAccessible( true );
      HashMap<String, LinkedList<AppConfigurationEntry>> configs = (HashMap) f.get( config );
      f.setAccessible( false );

      if ( configs == null ) {
        throw new IllegalArgumentException( "JAAS configuration is not available" );
      }

      return configs;
    } catch ( Exception e ) {
      throw new IllegalStateException( "JAAS configuration could not be loaded at this time", e);
    }
  }

  private JaasRealm createJaasRealm( final String realmName, final List<AppConfigurationEntry> entries ) {
    return new JaasRealm() {
      @Override
      public String getName() {
        return realmName;
      }

      @Override
      public int getRank() {
        return 0;
      }

      @Override
      public AppConfigurationEntry[] getEntries() {
        return entries.toArray( new AppConfigurationEntry[entries.size()] );
      }
    };
  }

  private boolean isMaprShimActive( HadoopConfiguration hadoopConfiguration ) {
    final String hadoopVersion = hadoopConfiguration.getHadoopShim().getHadoopVersion();
    LOGGER.debug( "Active shim hadoop version is " + hadoopVersion );

    return hadoopVersion != null && hadoopVersion.contains( "mapr" );
  }
}
