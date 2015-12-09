/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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
import org.pentaho.bigdata.api.mapreduce.MapReduceExecutionException;
import org.pentaho.bigdata.api.mapreduce.MapReduceService;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 12/7/15.
 */
public class FutureMapReduceJobSimpleImplTest {
  private ExecutorService executorService;
  private FutureMapReduceJobSimpleRunnable futureMapReduceJobSimpleRunnable;
  private AtomicBoolean complete;
  private AtomicInteger status;
  private AtomicReference<MapReduceExecutionException> exceptionAtomicReference;
  private Future future;
  private FutureMapReduceJobSimpleImpl futureMapReduceJobSimple;
  private MapReduceService.Stoppable stoppable;
  private int timeout;
  private TimeUnit unit;

  @Before
  public void setup() {
    executorService = mock( ExecutorService.class );
    future = mock( Future.class );
    stoppable = mock( MapReduceService.Stoppable.class );
    complete = new AtomicBoolean( false );
    status = new AtomicInteger( -1 );
    exceptionAtomicReference = new AtomicReference<>( null );
    timeout = 1000;
    unit = TimeUnit.SECONDS;

    futureMapReduceJobSimpleRunnable = mock( FutureMapReduceJobSimpleRunnable.class );
    when( futureMapReduceJobSimpleRunnable.getComplete() ).thenReturn( complete );
    when( futureMapReduceJobSimpleRunnable.getStatus() ).thenReturn( status );
    when( futureMapReduceJobSimpleRunnable.getExceptionAtomicReference() ).thenReturn( exceptionAtomicReference );

    when( executorService.submit( futureMapReduceJobSimpleRunnable ) ).thenReturn( future );

    futureMapReduceJobSimple =
      new FutureMapReduceJobSimpleImpl( executorService, this.getClass(), futureMapReduceJobSimpleRunnable );
  }

  @Test
  public void testSimpleConstructor() throws IOException {
    futureMapReduceJobSimple = new FutureMapReduceJobSimpleImpl( executorService, this.getClass(), "test args" );
    assertFalse( futureMapReduceJobSimple.isComplete() );
    assertFalse( futureMapReduceJobSimple.isSuccessful() );
    assertEquals( -1, futureMapReduceJobSimple.getStatus() );
    assertEquals( getClass().getCanonicalName(), futureMapReduceJobSimple.getMainClass() );
  }

  @Test
  public void testKillJobFirst() throws IOException {
    futureMapReduceJobSimple.killJob();
    assertTrue( futureMapReduceJobSimple.isComplete() );
    assertFalse( futureMapReduceJobSimple.isSuccessful() );
    verify( future ).cancel( true );
  }

  @Test
  public void testKillJobComplete() throws IOException {
    complete.set( true );
    status.set( 0 );
    futureMapReduceJobSimple.killJob();
    assertTrue( futureMapReduceJobSimple.isSuccessful() );
    verify( future, never() ).cancel( true );
  }

  @Test
  public void testWaitOnCompletionInterrupted()
    throws InterruptedException, ExecutionException, TimeoutException, IOException, MapReduceExecutionException {
    when( future.get( timeout, unit ) ).thenThrow( new InterruptedException() );
    assertTrue( futureMapReduceJobSimple.waitOnCompletion( timeout, unit, stoppable ) );
    verify( future ).cancel( true );
  }

  @Test( expected = MapReduceExecutionException.class )
  public void testWaitOnCompletionMapReduceExceptionThrown()
    throws InterruptedException, ExecutionException, TimeoutException, MapReduceExecutionException, IOException {
    MapReduceExecutionException mapReduceExecutionException = new MapReduceExecutionException( "" );
    when( future.get( timeout, unit ) ).thenThrow( new ExecutionException( mapReduceExecutionException ) );
    try {
      futureMapReduceJobSimple.waitOnCompletion( timeout, unit, stoppable );
    } catch ( MapReduceExecutionException e ) {
      assertEquals( mapReduceExecutionException, e );
      throw e;
    }
  }

  @Test( expected = MapReduceExecutionException.class )
  public void testWaitOnCompletionMapReduceExceptionSet()
    throws InterruptedException, ExecutionException, TimeoutException, MapReduceExecutionException, IOException {
    MapReduceExecutionException mapReduceExecutionException = new MapReduceExecutionException( "" );
    exceptionAtomicReference.set( mapReduceExecutionException );
    try {
      futureMapReduceJobSimple.waitOnCompletion( timeout, unit, stoppable );
    } catch ( MapReduceExecutionException e ) {
      assertEquals( mapReduceExecutionException, e );
      throw e;
    }
  }

  @Test( expected = MapReduceExecutionException.class )
  public void testWaitOnCompletionRuntimeException()
    throws InterruptedException, ExecutionException, TimeoutException, MapReduceExecutionException, IOException {
    RuntimeException runtimeException = new RuntimeException();
    when( future.get( timeout, unit ) ).thenThrow( new ExecutionException( runtimeException ) );
    try {
      futureMapReduceJobSimple.waitOnCompletion( timeout, unit, stoppable );
    } catch ( MapReduceExecutionException e ) {
      assertEquals( runtimeException, e.getCause() );
      throw e;
    }
  }

  @Test
  public void testWaitOnCompletionTimeout()
    throws InterruptedException, ExecutionException, TimeoutException, IOException, MapReduceExecutionException {
    when( future.get( timeout, unit ) ).thenThrow( new TimeoutException() );
    assertFalse( futureMapReduceJobSimple.waitOnCompletion( timeout, unit, stoppable ) );
  }

  @Test
  public void testWaitOnCompletionSuccess() throws IOException, MapReduceExecutionException {
    assertTrue( futureMapReduceJobSimple.waitOnCompletion( timeout, unit, stoppable ) );
  }
}
