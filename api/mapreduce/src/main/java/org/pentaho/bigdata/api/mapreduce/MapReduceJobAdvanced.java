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

/**
 * MapReduce job interface that supports progress monitoring, completion events, and task diagnostics
 */
public interface MapReduceJobAdvanced extends MapReduceJob {
  /**
   * Returns the setup progress
   *
   * @return the setup progress
   * @throws IOException
   */
  double getSetupProgress() throws IOException;

  /**
   * Returns the map progress
   *
   * @return the map progress
   * @throws IOException
   */
  double getMapProgress() throws IOException;

  /**
   * Returns the reduce progress
   *
   * @return the reduce progress
   * @throws IOException
   */
  double getReduceProgress() throws IOException;

  /**
   * Returns the TaskCompletionEvents starting at the given start index
   *
   * @param startIndex the start index
   * @return the TaskCompletionEvents starting at the given start index
   * @throws IOException
   */
  TaskCompletionEvent[] getTaskCompletionEvents( int startIndex ) throws IOException;

  /**
   * Returns the task diagnostics for a given attempt id
   *
   * @param taskAttemptId the attempt id
   * @return the task diagnostics for a given attempt id
   * @throws IOException
   */
  String[] getTaskDiagnostics( Object taskAttemptId ) throws IOException;
}
