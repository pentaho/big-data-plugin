/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

package org.pentaho.hadoop.shim.common.osgi.jaas;

import com.sun.security.auth.login.ConfigFile;
import org.apache.karaf.jaas.config.JaasRealm;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.di.core.hadoop.HadoopConfigurationListener;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.AppConfigurationEntry;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Class used to append jaas configuration set via <code>java.security.auth.login.config</code> system property to
 * currently active jaas configuration.
 */
public class JaasRealmsRegistrar implements HadoopConfigurationListener {
  private static final Logger LOGGER = LoggerFactory.getLogger( JaasRealmsRegistrar.class );
  private BundleContext bundleContext;
  private List<ServiceRegistration> realmRegistrations;

  public JaasRealmsRegistrar( BundleContext bundleContext ) throws ConfigurationException {
    this.bundleContext = bundleContext;
    HadoopConfigurationBootstrap hcb = HadoopConfigurationBootstrap.getInstance();
    hcb.registerHadoopConfigurationListener( this );
    hcb.notifyDependencyLoaded();
  }

  @Override public void onClassLoaderAvailable( ClassLoader classLoader ) {
    final ClassLoader originalCL = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader( classLoader );

      HashMap<String, LinkedList<AppConfigurationEntry>> configs = new HashMap<>();

      configs.putAll( getOverridenDefaultConfigs() );

      if ( isMaprShimActive( HadoopConfigurationBootstrap.getInstance() ) ) {
        configs.putAll( getMaprJaasConfig() );
      } else {
        LOGGER.info( "Active hadoop configuration is not MapR. Skipping JAAS realms registration." );
      }

      realmRegistrations = new ArrayList<>( configs.size() );

      for ( final String realmName : configs.keySet() ) {
        JaasRealm realm = createJaasRealm( realmName, configs.get( realmName ) );
        ServiceRegistration reg = getBundleContext().registerService( JaasRealm.class.getCanonicalName(), realm, null );
        realmRegistrations.add( reg );
      }

      LOGGER.debug( String.format( "Registered %s JAAS realms using system properties.", realmRegistrations.size() ) );
    } catch ( Exception e ) {
      LOGGER.error( "Error during setting up MapR JAAS configuration", e );
    } finally {
      Thread.currentThread().setContextClassLoader( originalCL );
    }
  }

  private HashMap<String, LinkedList<AppConfigurationEntry>> getOverridenDefaultConfigs() {
    HashMap<String, LinkedList<AppConfigurationEntry>> configs = new HashMap<>();

    HashMap<String, String> options = new HashMap<>();
    LinkedList<AppConfigurationEntry> entries = new LinkedList<>();

    options.put( "useTicketCache", "true" );
    options.put( "doNotPrompt", "true" );
    entries.add( new AppConfigurationEntry( "com.sun.security.auth.module.Krb5LoginModule",
      AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options ) );
    configs.put( "com.sun.security.jgss.krb5.initiate", entries );

    return configs;
  }

  @Override public void onConfigurationOpen( HadoopConfiguration hadoopConfiguration, boolean defaultConfiguration ) {
    // Noop
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
      if ( Const.isEmpty( System.getProperty( "java.security.auth.login.config" ) ) ) {
        // By the time mapr client didn't set environment variables we need, so do it manually
        Thread.currentThread().getContextClassLoader().loadClass( "com.mapr.baseutils.JVMProperties" ).newInstance();
      }

      Object config = new ConfigFile();
      try {
        Field spi = config.getClass().getDeclaredField( "spi" );
        boolean accessible = spi.isAccessible();
        if ( !accessible ) {
          spi.setAccessible( true );
        }
        try {
          config = spi.get( config );
        } finally {
          if ( !accessible ) {
            spi.setAccessible( accessible );
          }
        }
      } catch ( NoSuchFieldException e ) {
        // ignore
      }
      Field f = config.getClass().getDeclaredField( "configuration" );

      HashMap<String, LinkedList<AppConfigurationEntry>> configs;
      boolean accessible = f.isAccessible();
      if ( !accessible ) {
        f.setAccessible( true );
      }
      try {
        configs = (HashMap) f.get( config );
      } finally {
        if ( !accessible ) {
          f.setAccessible( accessible );
        }
      }

      if ( configs == null ) {
        throw new IllegalArgumentException( "JAAS configuration is not available" );
      }

      return configs;
    } catch ( Exception e ) {
      throw new IllegalStateException( "JAAS configuration could not be loaded at this time", e );
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
        return entries.toArray( new AppConfigurationEntry[ entries.size() ] );
      }
    };
  }

  boolean isMaprShimActive( HadoopConfigurationBootstrap hadoopConfigurationBootstrap ) {
    try {
      final String configurationId = hadoopConfigurationBootstrap.getActiveConfigurationId();
      LOGGER.debug( "Active shim configuration is " + configurationId );

      return configurationId != null && configurationId.matches( "mapr\\d+" );
    } catch ( ConfigurationException e ) {
      e.printStackTrace();
      return false;
    }
  }
}
