/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package org.pentaho.big.data.impl.shim.jaas;

import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.*;

public class JaasConfigServiceImplTest {

  @Test
  public void testEmptyPrincipalIsNotKerberos() throws Exception {
    Properties configProperties = new Properties();
    configProperties.setProperty( JaasConfigServiceImpl.KERBEROS_KEYTAB, "/user/path/file.keytab" );
    JaasConfigServiceImpl service = new JaasConfigServiceImpl( configProperties );
    assertFalse( service.isKerberos() );
  }

  @Test
  public void testEmptyKeytabIsNotKerberos() throws Exception {
    Properties configProperties = new Properties();
    configProperties.setProperty( JaasConfigServiceImpl.KERBEROS_PRINCIPAL, "me@host.com" );
    JaasConfigServiceImpl service = new JaasConfigServiceImpl( configProperties );
    assertFalse( service.isKerberos() );
  }

  @Test
  public void testJaasWithKerberosKeytab() throws Exception {
    Properties configProperties = new Properties();
    configProperties.setProperty( JaasConfigServiceImpl.KERBEROS_PRINCIPAL, "user@domain.com" );
    configProperties.setProperty( JaasConfigServiceImpl.KERBEROS_KEYTAB, "/user/path/file.keytab" );
    JaasConfigServiceImpl service = new JaasConfigServiceImpl( configProperties );
    assertTrue( service.isKerberos() );
    assertEquals(
      "com.sun.security.auth.module.Krb5LoginModule required\n"
        + "useKeyTab=true\n"
        + "serviceName=kafka\n"
        + "keyTab=\"/user/path/file.keytab\"\n"
        + "principal=\"user@domain.com\";",
      service.getJaasConfig() );
  }
}
