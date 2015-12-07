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
import org.pentaho.hadoop.shim.api.mapred.TaskCompletionEvent;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 12/8/15.
 */
public class TaskCompletionEventImplTest {
  private TaskCompletionEvent delegate;
  private TaskCompletionEventImpl taskCompletionEvent;

  @Before
  public void setup() {
    delegate = mock( TaskCompletionEvent.class );
    taskCompletionEvent = new TaskCompletionEventImpl( delegate );
  }

  @Test
  public void testGetTaskStatus() {
    for ( TaskCompletionEvent.Status status : TaskCompletionEvent.Status.values() ) {
      delegate = mock( TaskCompletionEvent.class );
      taskCompletionEvent = new TaskCompletionEventImpl( delegate );
      when( delegate.getTaskStatus() ).thenReturn( status );
      assertEquals( status.name(), taskCompletionEvent.getTaskStatus().name() );
    }
  }

  @Test
  public void testGetTaskAttemptId() {
    Object value = new Object();
    when( delegate.getTaskAttemptId() ).thenReturn( value );
    assertEquals( value, taskCompletionEvent.getTaskAttemptId() );
  }

  @Test
  public void testGetEventId() {
    int eventId = 30984;
    when( delegate.getEventId() ).thenReturn( eventId );
    assertEquals( eventId, taskCompletionEvent.getEventId() );
  }
}
