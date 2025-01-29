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

package org.pentaho.big.data.impl.shim.jaas;

import org.junit.Test;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class JaasConfigServiceFactoryTest {

  @Test
  public void testCreatesAJaasConfig() {
    NamedCluster namedCluster = mock( NamedCluster.class );
    JaasConfigServiceFactory factory = new JaasConfigServiceFactory( true, null );
    Properties configProperties = new Properties();
    configProperties.setProperty( JaasConfigServiceImpl.KERBEROS_PRINCIPAL, "three@domain.com" );
    configProperties.setProperty( JaasConfigServiceImpl.KERBEROS_KEYTAB, "/user/two/file.keytab" );
    assertTrue( factory.canHandle( namedCluster ) );
    assertEquals( "com.sun.security.auth.module.Krb5LoginModule required\n"
      + "useKeyTab=true\n"
      + "serviceName=kafka\n"
      + "keyTab=\"/user/two/file.keytab\"\n"
      + "principal=\"three@domain.com\";",
      factory.create( namedCluster ).getJaasConfig() );


  }
}
