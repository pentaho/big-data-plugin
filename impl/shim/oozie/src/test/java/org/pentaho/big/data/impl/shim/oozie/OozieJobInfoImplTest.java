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
import org.apache.oozie.client.WorkflowJob;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class OozieJobInfoImplTest {

  @Mock OozieClient client;
  @Mock WorkflowJob workflowJob;
  @Mock org.apache.oozie.client.OozieClientException exception;
  String id = "ID";
  OozieJobInfoImpl oozieJobInfo;

  @Before
  public void before() throws OozieClientException {
    oozieJobInfo = new OozieJobInfoImpl( id, client );
    when( client.getJobInfo( id ) ).thenReturn( workflowJob );
  }

  @Test
  public void testDidSucceed() throws Exception {
    when( workflowJob.getStatus() ).thenReturn( WorkflowJob.Status.SUCCEEDED );
    assertTrue( oozieJobInfo.didSucceed() );
  }

  @Test
  public void testDidntSucceed() throws Exception {
    when( workflowJob.getStatus() ).thenReturn( WorkflowJob.Status.FAILED );
    assertFalse( oozieJobInfo.didSucceed() );
  }

  @Test
  public void testGetId() throws Exception {
    assertThat( oozieJobInfo.getId(), is( id ) );
  }

  @Test
  public void testGetJobLog() throws Exception {
    when( client.getJobLog( id ) ).thenReturn( "JOB LOG" );
    assertThat( oozieJobInfo.getJobLog(),
      is( "JOB LOG" ) );
  }

  @Test
  public void testClientThrows() throws OozieClientException {
    when( client.getJobInfo( id ) ).thenThrow( exception );
    when( client.getJobLog( id ) ).thenThrow( exception );
    try {
      oozieJobInfo.didSucceed();
      fail( "expected exception" );
    } catch ( Exception e ) {
      assertThat( e, instanceOf( org.pentaho.oozie.shim.api.OozieClientException.class ) );
    }
    try {
      oozieJobInfo.getJobLog();
      fail( "expected exception" );
    } catch ( Exception e ) {
      assertThat( e, instanceOf( org.pentaho.oozie.shim.api.OozieClientException.class ) );
    }
    try {
      oozieJobInfo.isRunning();
      fail( "expected exception" );
    } catch ( Exception e ) {
      assertThat( e, instanceOf( org.pentaho.oozie.shim.api.OozieClientException.class ) );
    }
  }

  @Test
  public void testIsRunning() throws Exception {
    when( workflowJob.getStatus() ).thenReturn( WorkflowJob.Status.RUNNING );
    assertThat( oozieJobInfo.isRunning(), is( true ) );

  }
}
