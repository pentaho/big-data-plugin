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

import org.pentaho.bigdata.api.mapreduce.MapReduceJobAdvanced;
import org.pentaho.bigdata.api.mapreduce.MapReduceService;
import org.pentaho.bigdata.api.mapreduce.TaskCompletionEvent;
import org.pentaho.hadoop.shim.api.mapred.RunningJob;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by bryan on 12/3/15.
 */
public class RunningJobMapReduceJobAdvancedImpl implements MapReduceJobAdvanced {
  private final RunningJob runningJob;

  public RunningJobMapReduceJobAdvancedImpl( RunningJob runningJob ) {
    this.runningJob = runningJob;
  }

  @Override public void killJob() throws IOException {
    runningJob.killJob();
  }

  @Override public boolean waitOnCompletion( long timeout, TimeUnit timeUnit, MapReduceService.Stoppable stoppable )
    throws IOException, InterruptedException {
    long stopTime = System.currentTimeMillis() + timeUnit.toMillis( timeout );
    long sleepTime;
    while ( !stoppable.isStopped() && ( sleepTime = Math.min( 50, stopTime - System.currentTimeMillis() ) ) > 0
      && !runningJob.isComplete() ) {
      Thread.sleep( Math.max( 0, sleepTime ) );
    }
    return runningJob.isComplete();
  }

  @Override public double getSetupProgress() throws IOException {
    return runningJob.setupProgress();
  }

  @Override public double getMapProgress() throws IOException {
    return runningJob.mapProgress();
  }

  @Override public double getReduceProgress() throws IOException {
    return runningJob.reduceProgress();
  }

  @Override public boolean isSuccessful() throws IOException {
    return runningJob.isSuccessful();
  }

  @Override public boolean isComplete() throws IOException {
    return runningJob.isComplete();
  }

  @Override public TaskCompletionEvent[] getTaskCompletionEvents( int startIndex ) throws IOException {
    final org.pentaho.hadoop.shim.api.mapred.TaskCompletionEvent[] taskCompletionEvents =
      runningJob.getTaskCompletionEvents( startIndex );

    TaskCompletionEvent[] result = new TaskCompletionEvent[ taskCompletionEvents.length ];
    for ( int i = 0; i < taskCompletionEvents.length; i++ ) {
      result[ i ] = new TaskCompletionEventImpl( taskCompletionEvents[ i ] );
    }
    return result;
  }

  @Override public String[] getTaskDiagnostics( Object taskAttemptId ) throws IOException {
    return runningJob.getTaskDiagnostics( taskAttemptId );
  }
}
