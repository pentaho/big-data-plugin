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

/**
 * Created by bryan on 12/3/15.
 */
public interface TaskCompletionEvent {
  /**
   * Returns the status of the event
   *
   * @return the status of the event
   */
  TaskCompletionEvent.Status getTaskStatus();

  /**
   * Returns the task attempt id
   *
   * @return the task attempt id
   */
  Object getTaskAttemptId();

  /**
   * Returns the event id
   *
   * @return the event id
   */
  int getEventId();

  /**
   * Enumeration of possible status codes
   */
  enum Status {
    FAILED,
    KILLED,
    SUCCEEDED,
    OBSOLETE,
    TIPFAILED;
  }
}
