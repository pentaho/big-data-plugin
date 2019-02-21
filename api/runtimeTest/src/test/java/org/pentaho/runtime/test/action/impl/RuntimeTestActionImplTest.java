/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.runtime.test.action.impl;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.runtime.test.action.RuntimeTestActionPayload;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Created by bryan on 9/10/15.
 */
public class RuntimeTestActionImplTest {
  private String name;
  private String description;
  private RuntimeTestEntrySeverity severity;
  private RuntimeTestActionPayload payload;
  private RuntimeTestActionImpl runtimeTestAction;

  @Before
  public void setup() {
    name = "name";
    description = "description";
    severity = RuntimeTestEntrySeverity.DEBUG;
    payload = mock( RuntimeTestActionPayload.class );
    runtimeTestAction = new RuntimeTestActionImpl( name, description, severity, payload );
  }

  @Test
  public void testGetName() {
    assertEquals( name, runtimeTestAction.getName() );
  }

  @Test
  public void testGetDescription() {
    assertEquals( description, runtimeTestAction.getDescription() );
  }

  @Test
  public void testGetSeverity() {
    assertEquals( severity, runtimeTestAction.getSeverity() );
  }

  @Test
  public void testGetPayload() {
    assertEquals( payload, runtimeTestAction.getPayload() );
  }
}
