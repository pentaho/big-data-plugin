/*******************************************************************************
 * Pentaho Big Data
 * <p/>
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
 * <p/>
 * ******************************************************************************
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 ******************************************************************************/

package org.pentaho.big.data.impl.shim.oozie;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.bigdata.api.oozie.OozieServiceException;
import org.pentaho.oozie.shim.api.OozieClient;
import org.pentaho.oozie.shim.api.OozieClientException;

import java.util.Properties;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith( MockitoJUnitRunner.class )
public class OozieServiceImplTest {

  @Mock OozieClient client;
  @Mock Properties properties;
  @InjectMocks OozieServiceImpl serviceImpl;

  @Test
  public void testGetClientBuildVersion() throws Exception {
    serviceImpl.getClientBuildVersion();
    verify( client ).getClientBuildVersion();
  }

  @Test
  public void testGetProtocolUrl() throws Exception {
    serviceImpl.getProtocolUrl();
    verify( client ).getProtocolUrl();
  }

  @Test
  public void testHasAppPath() throws Exception {
    assertThat(
      serviceImpl.hasAppPath( properties ),
      is( false ) );
    Properties props = new Properties();
    props.put( org.apache.oozie.client.OozieClient.APP_PATH,
      "present" );
    assertThat(
      serviceImpl.hasAppPath( props ),
      is( true ) );
  }

  @Test
  public void testRun() throws Exception {
    serviceImpl.run( properties );
    verify( client ).run( properties );
  }

  @Test
  public void testFailedRun() throws OozieClientException {
    when( client.run( properties ) )
      .thenThrow( mock( OozieClientException.class ) );
    try {
      serviceImpl.run( properties );
      fail( "expected exception" );
    } catch ( OozieServiceException e ) {
      assertThat( e, instanceOf( OozieServiceException.class ) );
    }
  }

  @Test
  public void testFailedGetUrl() throws OozieClientException {
    when( client.getProtocolUrl() )
      .thenThrow( mock( OozieClientException.class ) );
    try {
      serviceImpl.getProtocolUrl();
      fail( "expected exception" );
    } catch ( OozieServiceException e ) {
      assertThat( e, instanceOf( OozieServiceException.class ) );
    }
  }

  @Test
  public void testValidateWSVersion() throws Exception {
    serviceImpl.validateWSVersion();
    verify( client ).validateWSVersion();
  }

  @Test
  public void testFailedValidate() throws OozieClientException {
    doThrow( mock( OozieClientException.class ) )
      .when( client ).validateWSVersion();
    try {
      serviceImpl.validateWSVersion();
      fail( "expected exception" );
    } catch ( OozieServiceException e ) {
      assertThat( e, instanceOf( OozieServiceException.class ) );
    }
  }

}
