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

import org.apache.oozie.client.OozieClientException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.bigdata.api.oozie.OozieServiceException;
import org.pentaho.oozie.shim.api.OozieJob;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class OozieJobInfoDelegateTest {

  @Mock OozieJob job;
  @Mock org.pentaho.oozie.shim.api.OozieClientException exception;
  String id = "ID";
  OozieJobInfoDelegate oozieJobInfoDelegate;

  @Before
  public void before() throws OozieClientException {
    oozieJobInfoDelegate = new OozieJobInfoDelegate( job );
  }

  @Test
  public void testDidSucceed() throws Exception {
    when( job.didSucceed() ).thenReturn( true );
    assertTrue( oozieJobInfoDelegate.didSucceed() );
  }

  @Test
  public void testDidntSucceed() throws Exception {
    when( job.didSucceed() ).thenReturn( false );
    assertFalse( oozieJobInfoDelegate.didSucceed() );
  }

  @Test
  public void testGetId() throws Exception {
    when( job.getId() ).thenReturn( id );
    assertThat( oozieJobInfoDelegate.getId(), is( id ) );
  }

  @Test
  public void testGetJobLog() throws Exception {
    when( job.getJobLog() ).thenReturn( "JOB LOG" );
    assertThat( oozieJobInfoDelegate.getJobLog(),
      is( "JOB LOG" ) );
  }

  @Test
  public void testClientThrows() throws org.pentaho.oozie.shim.api.OozieClientException {
    when( job.didSucceed() ).thenThrow( exception );
    when( job.isRunning() ).thenThrow( exception );
    when( job.getJobLog() ).thenThrow( exception );
    try {
      oozieJobInfoDelegate.didSucceed();
      fail( "expected exception" );
    } catch ( Exception e ) {
      assertThat( e, instanceOf( OozieServiceException.class ) );
    }
    try {
      oozieJobInfoDelegate.getJobLog();
      fail( "expected exception" );
    } catch ( Exception e ) {
      assertThat( e, instanceOf( OozieServiceException.class ) );
    }
    try {
      oozieJobInfoDelegate.isRunning();
      fail( "expected exception" );
    } catch ( Exception e ) {
      assertThat( e, instanceOf( OozieServiceException.class ) );
    }
  }

  @Test
  public void testIsRunning() throws Exception {
    when( job.isRunning() ).thenReturn( true );
    assertThat( oozieJobInfoDelegate.isRunning(), is( true ) );

  }
}
