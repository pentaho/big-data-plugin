/*******************************************************************************
 * Pentaho Big Data
 * <p/>
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
 * <p/>
 * ******************************************************************************
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package org.pentaho.hadoop.shim.common.osgi.jaas;

import static org.mockito.Mockito.*;

import java.net.URL;
import java.util.Dictionary;

import org.apache.karaf.jaas.config.JaasRealm;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.spi.HadoopShim;

import com.google.common.base.Strings;

public class JaasRealmsRegistrarTest {
  private static final String JAAS_CONFIG_PROP_NAME = "java.security.auth.login.config";
  private String jaasConfLocationBackup;
  private JaasRealmsRegistrar testee;

  @Before
  public void setup() throws ConfigurationException {
    jaasConfLocationBackup = System.getProperty( JAAS_CONFIG_PROP_NAME );
    testee = mock( JaasRealmsRegistrar.class );
    doCallRealMethod().when( testee ).onClassLoaderAvailable( any( ClassLoader.class ) );
    doCallRealMethod().when( testee ).onConfigurationClose( any( HadoopConfiguration.class ) );
    doReturn( true ).when( testee ).isMaprShimActive( any( HadoopConfigurationBootstrap.class ) );
  }

  @After
  public void cleanup() {
    if ( !Strings.isNullOrEmpty( jaasConfLocationBackup ) ) {
      System.setProperty( JAAS_CONFIG_PROP_NAME, jaasConfLocationBackup );
    } else {
      System.clearProperty( JAAS_CONFIG_PROP_NAME );
    }
  }

  @Test
  public void testIgnoresNotMaprShims() throws ConfigurationException {
    doReturn( true ).when( testee ).isMaprShimActive( any( HadoopConfigurationBootstrap.class ) );
    BundleContext bundleContext = mock( BundleContext.class );
    doReturn( bundleContext ).when( testee ).getBundleContext();
    testee.onClassLoaderAvailable( mock( ClassLoader.class ) );
    verify( bundleContext, never() ).registerService( anyString(), any( JaasRealm.class ), isNull( Dictionary.class ) );
  }

  @Test
  public void testRegistersAndUnregistersRealms() {
    URL confURL = getClass().getClassLoader().getResource( "dummy-jaas.conf" );
    System.setProperty( JAAS_CONFIG_PROP_NAME, confURL.toString() );
    BundleContext bundleContext = mock( BundleContext.class );
    ServiceRegistration realmRegistration = mock( ServiceRegistration.class );
    doReturn( realmRegistration ).when( bundleContext ).registerService( anyString(), any( JaasRealm.class ),
        isNull( Dictionary.class ) );
    doReturn( bundleContext ).when( testee ).getBundleContext();
    HadoopConfiguration hadoopConf = createHadoopConfiguration( "2.5.1-mapr-1503" );
    testee.onClassLoaderAvailable( mock( ClassLoader.class ) );
    verify( bundleContext ).registerService( eq( JaasRealm.class.getCanonicalName() ),
        argThat( jaasRealmWithName( "mapr-jaas-config" ) ), isNull( Dictionary.class ) );
    testee.onConfigurationClose( hadoopConf );
    verify( realmRegistration, times( 2 ) ).unregister();
  }

  private HadoopConfiguration createHadoopConfiguration( String hadoopVersion ) {
    HadoopShim shim = mock( HadoopShim.class );
    when( shim.getHadoopVersion() ).thenReturn( hadoopVersion );

    HadoopConfiguration conf = mock( HadoopConfiguration.class );
    when( conf.getHadoopShim() ).thenReturn( shim );

    return conf;
  }

  private Matcher jaasRealmWithName( final String realmName ) {
    return new BaseMatcher() {
      @Override
      public boolean matches( Object o ) {
        return o instanceof JaasRealm && ( (JaasRealm) o ).getName().equals( realmName );
      }

      @Override
      public void describeTo( Description description ) {
        description.appendText( String.format( "JaasRealm(name==\"%s\"", realmName ) );
      }
    };
  }
}
