/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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
package org.pentaho.big.data.impl.shim.oozie;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.oozie.shim.api.OozieJob;

import java.util.Properties;

import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.*;

@RunWith( MockitoJUnitRunner.class )
public class FallbackOozieClientImplTest {

  @Mock OozieClient delegate;
  @Mock Properties props;
  @InjectMocks FallbackOozieClientImpl fallbackClient;

  @Test
  public void testGetClientBuildVersion() throws Exception {
    fallbackClient.getClientBuildVersion();
    verify( delegate ).getClientBuildVersion();
  }

  @Test
  public void testGetJob() throws Exception {
    OozieJob job = fallbackClient.getJob( "Foo" );
    assertThat( job.getId(), is( "Foo" ) );
  }

  @Test
  public void testGetProtocolUrl() throws Exception {
    fallbackClient.getProtocolUrl();
    verify( delegate ).getProtocolUrl();
  }

  @Test
  public void testHasAppPath() throws Exception {
    Properties properties = new Properties();
    assertThat(
      fallbackClient.hasAppPath( properties ), is( false ) );
    properties.put( OozieClient.APP_PATH, "appPath" );
    assertThat(
      fallbackClient.hasAppPath( properties ), is( true ) );
  }

  @Test
  public void testRun() throws Exception {
    fallbackClient.run( props );
    verify( delegate ).run( props );
  }

  @Test
  public void testFailedRun() throws OozieClientException {
    when( delegate.run( props ) ).thenThrow( mock( OozieClientException.class ) );
    try {
      fallbackClient.run( props );
      fail( "expected exception" );
    } catch ( Exception e ) {
      assertThat( e, instanceOf( org.pentaho.oozie.shim.api.OozieClientException.class ) );
    }
  }

  @Test
  public void testFailedValidate() throws OozieClientException {
    doThrow( mock( OozieClientException.class ) )
      .when( delegate ).validateWSVersion();
    try {
      fallbackClient.validateWSVersion();
      fail( "expected exception" );
    } catch ( Exception e ) {
      assertThat( e, instanceOf( org.pentaho.oozie.shim.api.OozieClientException.class ) );
    }
  }

  @Test
  public void testFailedProtocolUrl() throws OozieClientException {
    doThrow( mock( OozieClientException.class ) )
      .when( delegate ).getProtocolUrl();
    try {
      fallbackClient.getProtocolUrl();
      fail( "expected exception" );
    } catch ( Exception e ) {
      assertThat( e, instanceOf( org.pentaho.oozie.shim.api.OozieClientException.class ) );
    }
  }

  @Test
  public void testValidateWSVersion() throws Exception {
    fallbackClient.validateWSVersion();
    verify( delegate ).validateWSVersion();
  }
}
