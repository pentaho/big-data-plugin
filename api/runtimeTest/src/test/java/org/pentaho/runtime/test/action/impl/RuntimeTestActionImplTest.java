/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
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
