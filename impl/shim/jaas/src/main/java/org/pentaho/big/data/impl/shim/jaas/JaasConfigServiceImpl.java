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
