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

package org.pentaho.big.data.impl.shim.mapreduce;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.bigdata.api.mapreduce.MapReduceService;
import org.pentaho.hadoop.shim.api.mapred.RunningJob;
import org.pentaho.hadoop.shim.api.mapred.TaskCompletionEvent;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 12/8/15.
 */
public class RunningJobMapReduceJobAdvancedImplTest {
  private RunningJob runningJob;
  private RunningJobMapReduceJobAdvancedImpl runningJobMapReduceJobAdvanced;
  private MapReduceService.Stoppable stoppable;

  @Before
  public void setup() {
    runningJob = mock( RunningJob.class );
    runningJobMapReduceJobAdvanced = new RunningJobMapReduceJobAdvancedImpl( runningJob );
    stoppable = mock( MapReduceService.Stoppable.class );
  }

  @Test
  public void testKillJob() throws IOException {
    runningJobMapReduceJobAdvanced.killJob();
    verify( runningJob ).killJob();
  }

  @Test( timeout = 500 )
  public void testWaitOnConCompletionStopped() throws IOException, InterruptedException {
    when( stoppable.isStopped() ).thenReturn( true );
    assertFalse( runningJobMapReduceJobAdvanced.waitOnCompletion( 10, TimeUnit.MINUTES, stoppable ) );
  }

  @Test( timeout = 500 )
  public void testWaitOnCompletionFalse() throws IOException, InterruptedException {
    assertFalse( runningJobMapReduceJobAdvanced.waitOnCompletion( 10, TimeUnit.MILLISECONDS, stoppable ) );
  }

  @Test( timeout = 500 )
  public void testWaitOnCompletionCompleteBeforeSleep() throws IOException, InterruptedException {
    when( runningJob.isComplete() ).thenReturn( true );
    assertTrue( runningJobMapReduceJobAdvanced.waitOnCompletion( 10, TimeUnit.MILLISECONDS, stoppable ) );
  }

  @Test( timeout = 500 )
  public void testWaitOnCompletionCompleteAfterSleep() throws IOException, InterruptedException {
    when( runningJob.isComplete() ).thenReturn( false, true );
    assertTrue( runningJobMapReduceJobAdvanced.waitOnCompletion( 10, TimeUnit.MILLISECONDS, stoppable ) );
  }

  @Test
  public void testGetSetupProgress() throws IOException {
    float setupProgress = 1.25f;
    when( runningJob.setupProgress() ).thenReturn( setupProgress );
    assertEquals( setupProgress, runningJobMapReduceJobAdvanced.getSetupProgress(), 0 );
  }

  @Test
  public void testGetMapProgress() throws IOException {
    float mapProgress = 1.25f;
    when( runningJob.mapProgress() ).thenReturn( mapProgress );
    assertEquals( mapProgress, runningJobMapReduceJobAdvanced.getMapProgress(), 0 );
  }

  @Test
  public void testGetReduceProgress() throws IOException {
    float reduceProgress = 1.25f;
    when( runningJob.reduceProgress() ).thenReturn( reduceProgress );
    assertEquals( reduceProgress, runningJobMapReduceJobAdvanced.getReduceProgress(), 0 );
  }

  @Test
  public void testIsSuccessful() throws IOException {
    when( runningJob.isSuccessful() ).thenReturn( true, false );
    assertTrue( runningJobMapReduceJobAdvanced.isSuccessful() );
    assertFalse( runningJobMapReduceJobAdvanced.isSuccessful() );
  }

  @Test
  public void testIsComplete() throws IOException {
    when( runningJob.isComplete() ).thenReturn( true, false );
    assertTrue( runningJobMapReduceJobAdvanced.isComplete() );
    assertFalse( runningJobMapReduceJobAdvanced.isComplete() );
  }

  @Test
  public void testGetTaskCompletionEvents() throws IOException {
    int id = 256;
    TaskCompletionEvent taskCompletionEvent = mock( TaskCompletionEvent.class );
    when( runningJob.getTaskCompletionEvents( 1 ) )
      .thenReturn( new TaskCompletionEvent[] { taskCompletionEvent } );
    when( taskCompletionEvent.getEventId() ).thenReturn( id );
    org.pentaho.bigdata.api.mapreduce.TaskCompletionEvent[] taskCompletionEvents =
      runningJobMapReduceJobAdvanced.getTaskCompletionEvents( 1 );
    assertEquals( 1, taskCompletionEvents.length );
    assertEquals( id, taskCompletionEvents[ 0 ].getEventId() );
  }

  @Test
  public void testGetTaskDiagnostics() throws IOException {
    Object o = new Object();
    String[] value = { "diag" };
    when( runningJob.getTaskDiagnostics( o ) ).thenReturn( value );
    assertArrayEquals( value, runningJobMapReduceJobAdvanced.getTaskDiagnostics( o ) );
  }
}
