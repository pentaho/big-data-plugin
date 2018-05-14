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

import org.pentaho.hadoop.shim.api.jaas.JaasConfigService;

import java.util.Properties;

public class JaasConfigServiceImpl implements JaasConfigService {
  public static final String KERBEROS_PRINCIPAL = "pentaho.authentication.default.kerberos.principal";
  public static final String KERBEROS_KEYTAB = "pentaho.authentication.default.kerberos.keytabLocation";
  private Properties configProperties;

  public JaasConfigServiceImpl( Properties configProperties ) {

    this.configProperties = configProperties;
  }

  @Override public String getJaasConfig() {
    return
      "com.sun.security.auth.module.Krb5LoginModule required\n"
        + "useKeyTab=true\n"
        + "serviceName=kafka\n"
        + "keyTab=\"" + configProperties.getProperty( KERBEROS_KEYTAB ) + "\"\n"
        + "principal=\"" + configProperties.getProperty( KERBEROS_PRINCIPAL ) + "\";";
  }

  @Override public boolean isKerberos() {
    Object principal = configProperties.get( KERBEROS_PRINCIPAL );
    Object keytab = configProperties.get( KERBEROS_KEYTAB );
    return principal != null && keytab != null && !"".equals( principal ) && !"".equals( keytab );
  }
}
