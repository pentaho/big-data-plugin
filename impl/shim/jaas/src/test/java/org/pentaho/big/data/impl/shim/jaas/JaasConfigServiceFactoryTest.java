/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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
