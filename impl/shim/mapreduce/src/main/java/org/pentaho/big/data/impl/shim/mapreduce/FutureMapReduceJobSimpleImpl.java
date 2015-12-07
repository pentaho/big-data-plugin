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

import org.pentaho.bigdata.api.mapreduce.MapReduceExecutionException;
import org.pentaho.bigdata.api.mapreduce.MapReduceJobSimple;
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

/**
 * Created by bryan on 12/4/15.
 */
public class FutureMapReduceJobSimpleImpl implements MapReduceJobSimple {
  private final Future<?> future;
  private final String mainClass;
  private final AtomicBoolean complete;
  private final AtomicInteger status;
  private final AtomicReference<MapReduceExecutionException> exceptionAtomicReference;

  public FutureMapReduceJobSimpleImpl( ExecutorService executorService, final Class<?> mainClass,
                                       final String commandLineArgs ) {
    this( executorService, mainClass,
      new FutureMapReduceJobSimpleRunnable( mainClass, commandLineArgs, new AtomicBoolean( false ),
        new AtomicInteger( -1 ), new AtomicReference<MapReduceExecutionException>( null ) ) );
  }

  public FutureMapReduceJobSimpleImpl( ExecutorService executorService, final Class<?> mainClass,
                                       FutureMapReduceJobSimpleRunnable runnable ) {
    this.mainClass = mainClass.getCanonicalName();
    this.future = executorService.submit( runnable );
    this.complete = runnable.getComplete();
    this.status = runnable.getStatus();
    this.exceptionAtomicReference = runnable.getExceptionAtomicReference();
  }

  @Override public void killJob() throws IOException {
    if ( !complete.getAndSet( true ) ) {
      future.cancel( true );
    }
  }

  @Override public boolean waitOnCompletion( long timeout, TimeUnit timeUnit, MapReduceService.Stoppable stoppable )
    throws IOException, MapReduceExecutionException {
    try {
      future.get( timeout, timeUnit );
      MapReduceExecutionException mapReduceExecutionException = exceptionAtomicReference.get();
      if ( mapReduceExecutionException != null ) {
        throw mapReduceExecutionException;
      }
      return true;
    } catch ( InterruptedException e ) {
      killJob();
      return true;
    } catch ( ExecutionException e ) {
      Throwable cause = e.getCause();
      if ( cause instanceof MapReduceExecutionException ) {
        throw (MapReduceExecutionException) cause;
      } else {
        throw new MapReduceExecutionException( cause );
      }
    } catch ( TimeoutException e ) {
      return false;
    }
  }

  @Override public boolean isSuccessful() throws IOException {
    return status.get() == 0;
  }

  @Override public boolean isComplete() throws IOException {
    return complete.get();
  }

  @Override public String getMainClass() {
    return mainClass;
  }

  @Override public int getStatus() {
    return status.get();
  }
}
