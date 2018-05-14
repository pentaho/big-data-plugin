/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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
