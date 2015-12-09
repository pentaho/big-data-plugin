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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by bryan on 12/8/15.
 */
public class MapReduceExecutionExceptionTest {
  @Test
  public void testStringConstructor() {
    String msg = "msg";
    assertEquals( msg, new MapReduceExecutionException( msg ).getMessage() );
  }

  @Test
  public void testThrowableConstructor() {
    Throwable throwable = new Throwable();
    assertEquals( throwable, new MapReduceExecutionException( throwable ).getCause() );
  }
}
