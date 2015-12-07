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

package org.pentaho.bigdata.api.mapreduce;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Common interface for a running MapReduce job.
 */
public interface MapReduceJob {
  /**
   * Kills the job
   *
   * @throws IOException
   */
  void killJob() throws IOException;

  /**
   * Wait for up to timeout time units (ex: 10 seconds) for the job to complete.
   *
   * @param timeout   timeout value
   * @param timeUnit  the time units of the timeout value
   * @param stoppable boolean callback taht can terminate the wait early (optional in implementations)
   * @return a boolean indicating whether the job completed during the wait
   * @throws IOException
   * @throws InterruptedException
   * @throws MapReduceExecutionException
   */
  boolean waitOnCompletion( long timeout, TimeUnit timeUnit, MapReduceService.Stoppable stoppable )
    throws IOException, InterruptedException, MapReduceExecutionException;

  /**
   * Returns a boolean indicating whether the MapReduce job was successful
   *
   * @return a boolean indicating whether the MapReduce job was successful
   * @throws IOException
   */
  boolean isSuccessful() throws IOException;

  /**
   * Returns a boolean indicating whether the MapReduce job is complete
   *
   * @return a boolean indicating whether the MapReduce job is complete
   * @throws IOException
   */
  boolean isComplete() throws IOException;
}
