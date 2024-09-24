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

package org.pentaho.big.data.kettle.plugins.hive;

import com.google.common.annotations.VisibleForTesting;
import org.pentaho.di.core.database.DatabaseMeta;

import static com.google.common.base.Strings.isNullOrEmpty;

public class SimbaUrl {

  @VisibleForTesting static final String KRB_HOST_FQDN = "KrbHostFQDN";
  @VisibleForTesting static final String KRB_SERVICE_NAME = "KrbServiceName";
  @VisibleForTesting static final String URL_IS_CONFIGURED_THROUGH_JNDI = "Url is configured through JNDI";

  final String jdbcPrefix;
  private String username;
  private String password;
  private boolean isKerberos;
  private int accessType;
  private int defaultPort;
  private String port;
  private String hostname;
  private String databaseName;

  private String jdbcUrlTemplate;
  private static final String DEFAULT_DB = "default";

  public SimbaUrl( Builder builder ) {
    this.jdbcPrefix = builder.jdbcPrefix;
    this.username = builder.username;
    this.password = builder.password;
    this.isKerberos = builder.isKerberos;
    this.accessType = builder.accessType;
    this.defaultPort = builder.defaultPort;
    this.port = builder.port;
    this.hostname = builder.hostname;
    this.databaseName = builder.databaseName;
    this.jdbcUrlTemplate = jdbcPrefix + "%s:%d/%s;AuthMech=%d%s";
  }

  public String getURL() {
    Integer portNumber;
    if ( isNullOrEmpty( port ) ) {
      portNumber = defaultPort;
    } else {
      portNumber = Integer.valueOf( port );
    }
    if ( isNullOrEmpty( databaseName ) ) {
      databaseName = DEFAULT_DB;
    }
    switch ( accessType ) {
      case DatabaseMeta.TYPE_ACCESS_JNDI: {
        return URL_IS_CONFIGURED_THROUGH_JNDI;
      }
      case DatabaseMeta.TYPE_ACCESS_NATIVE:
      default: {
        Integer authMethod = 0;
        StringBuilder additional = new StringBuilder();
        String userName = username;
        String password = this.password;
        if ( isKerberos ) {
          authMethod = 1;
        } else if ( !isNullOrEmpty( userName ) ) {
          additional.append( ";UID=" );
          additional.append( userName );
          if ( !isNullOrEmpty( password ) ) {
            authMethod = 3;
            additional.append( ";PWD=" );
            additional.append( password );
          } else {
            authMethod = 2;
          }
        }
        return String.format( jdbcUrlTemplate, hostname, portNumber, databaseName, authMethod, additional );
      }
    }
  }

  public static final class Builder {
    private String jdbcPrefix;
    private int accessType;
    private String databaseName;
    private int defaultPort;
    private String hostname;
    private boolean isKerberos;
    private String password;
    private String port;
    private String username;

    private Builder() {
    }

    public static Builder create() {
      return new Builder();
    }

    public Builder withAccessType( int accessType ) {
      this.accessType = accessType;
      return this;
    }

    public Builder withDatabaseName( String databaseName ) {
      this.databaseName = databaseName;
      return this;
    }

    public Builder withDefaultPort( int defaultPort ) {
      this.defaultPort = defaultPort;
      return this;
    }

    public Builder withHostname( String hostname ) {
      this.hostname = hostname;
      return this;
    }

    public Builder withIsKerberos( boolean isKerberos ) {
      this.isKerberos = isKerberos;
      return this;
    }

    public Builder withJdbcPrefix( String jdbcPrefix ) {
      this.jdbcPrefix = jdbcPrefix;
      return this;
    }

    public Builder withPassword( String password ) {
      this.password = password;
      return this;
    }

    public Builder withPort( String port ) {
      this.port = port;
      return this;
    }

    public Builder withUsername( String username ) {
      this.username = username;
      return this;
    }

    public SimbaUrl build() {
      SimbaUrl simbaUrl =
        new SimbaUrl( this );
      return simbaUrl;
    }
  }
}
