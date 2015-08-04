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

package org.pentaho.bigdata.api.pig.impl;

import org.apache.commons.vfs2.FileObject;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Created by bryan on 7/14/15.
 */
public class PigResultImplTest {
  @Test
  public void testConstructor() {
    FileObject logFile = mock( FileObject.class );
    int[] result = new int[] { 1, 2, 3 };
    Exception exception = mock( Exception.class );
    PigResultImpl pigResult = new PigResultImpl( logFile, result, exception );
    assertEquals( logFile, pigResult.getLogFile() );
    assertArrayEquals( result, pigResult.getResult() );
    assertEquals( exception, pigResult.getException() );
  }
}
