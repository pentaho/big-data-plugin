/*******************************************************************************
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

package org.pentaho.big.data.kettle.plugins.hive;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.pentaho.di.core.database.DatabaseMeta.TYPE_ACCESS_JNDI;
import static org.pentaho.di.core.database.DatabaseMeta.TYPE_ACCESS_NATIVE;
import static org.pentaho.di.core.database.DatabaseMeta.TYPE_ACCESS_ODBC;

public class SimbaUrlTest {

  SimbaUrl.Builder builder = SimbaUrl.Builder.create();

  @Test
  public void testWithDefaultPort() {
    assertThat(
      builder
        .withPort( "" )
        .withDefaultPort( 101010 )
        .withJdbcPrefix( "foo:bar://" )
        .withHostname( "localhost" )
        .build().getURL(),
      containsString( "foo:bar://localhost:101010/default" ) );
  }

  @Test
  public void testWithSetPort() {
    assertThat(
      builder
        .withPort( "" )
        .withDefaultPort( 101010 )
        .withPort( "202020" )
        .withJdbcPrefix( "foo:bar://" )
        .withHostname( "localhost" )
        .build().getURL(),
      containsString( "foo:bar://localhost:202020/default" ) );
  }

  @Test
  public void testWithDatabaseName() {
    assertThat(
      builder
        .withPort( "" )
        .withDefaultPort( 101010 )
        .withPort( "202020" )
        .withDatabaseName( "mydatabase" )
        .withJdbcPrefix( "foo:bar://" )
        .withHostname( "localhost" )
        .build().getURL(),
      containsString( "foo:bar://localhost:202020/mydatabase" ) );
  }

  @Test
  public void testOdbc() {
    assertThat(
      builder
        .withAccessType( TYPE_ACCESS_ODBC )
        .withDatabaseName( "mydatabase" )
        .build().getURL(),
      containsString( "jdbc:odbc:mydatabase" ) );
  }

  @Test
  public void testJndi() {
    assertThat(
      builder
        .withAccessType( TYPE_ACCESS_JNDI )
        .build().getURL(),
      is( SimbaUrl.URL_IS_CONFIGURED_THROUGH_JNDI ) );
  }

  @Test
  public void testAuthMech0() {
    assertThat(
      builder
        .withAccessType( TYPE_ACCESS_NATIVE )
        .withIsKerberos( false )
        .withPort( "202020" )
        .withDatabaseName( "mydatabase" )
        .withJdbcPrefix( "foo:bar://" )
        .withHostname( "localhost" )
        .build().getURL(),
      is( "foo:bar://localhost:202020/mydatabase;AuthMech=0" ) );
  }

  @Test
  public void testAuthMech1() {
    assertThat(
      builder
        .withAccessType( TYPE_ACCESS_NATIVE )
        .withIsKerberos( false )
        .withPort( "202020" )
        .withIsKerberos( true )
        .withDatabaseName( "mydatabase" )
        .withJdbcPrefix( "foo:bar://" )
        .withHostname( "localhost" )
        .build().getURL(),
      is( "foo:bar://localhost:202020/mydatabase;AuthMech=1" ) );
  }

  @Test
  public void testAuthMech2() {
    assertThat(
      builder
        .withAccessType( TYPE_ACCESS_NATIVE )
        .withIsKerberos( false )
        .withPort( "202020" )
        .withUsername( "user" )
        .withDatabaseName( "mydatabase" )
        .withJdbcPrefix( "foo:bar://" )
        .withHostname( "localhost" )
        .build().getURL(),
      is( "foo:bar://localhost:202020/mydatabase;AuthMech=2;UID=user" ) );
  }

  @Test
  public void testAuthMech3() {
    assertThat(
      builder
        .withAccessType( TYPE_ACCESS_NATIVE )
        .withIsKerberos( false )
        .withPort( "202020" )
        .withUsername( "user" )
        .withPassword( "password" )
        .withDatabaseName( "mydatabase" )
        .withJdbcPrefix( "foo:bar://" )
        .withHostname( "localhost" )
        .build().getURL(),
      is( "foo:bar://localhost:202020/mydatabase;AuthMech=3;UID=user;PWD=password" ) );
  }
}
