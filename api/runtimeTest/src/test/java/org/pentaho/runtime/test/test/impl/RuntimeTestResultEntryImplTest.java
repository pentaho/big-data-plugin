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

package org.pentaho.runtime.test.test.impl;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by bryan on 8/21/15.
 */
public class RuntimeTestResultEntryImplTest {

  private RuntimeTestEntrySeverity severity;
  private String description;
  private String message;
  private Exception exception;
  private RuntimeTestResultEntryImpl runtimeTestResultEntry;

  @Before
  public void setup() {
    severity = RuntimeTestEntrySeverity.ERROR;
    description = "desc";
    message = "msg";
    exception = new Exception();
    runtimeTestResultEntry = new RuntimeTestResultEntryImpl( severity, description, message, exception );
  }

  @Test
  public void test3ArgConstructor() {
    exception = null;
    runtimeTestResultEntry = new RuntimeTestResultEntryImpl( severity, description, message );
    testGetSeverity();
    testGetDescription();
    testGetMessage();
    testToString();
  }

  @Test
  public void testGetSeverity() {
    assertEquals( severity, runtimeTestResultEntry.getSeverity() );
  }

  @Test
  public void testGetDescription() {
    assertEquals( description, runtimeTestResultEntry.getDescription() );
  }

  @Test
  public void testGetMessage() {
    assertEquals( message, runtimeTestResultEntry.getMessage() );
  }

  @Test
  public void testGetException() {
    assertEquals( exception, runtimeTestResultEntry.getException() );
  }

  @Test
  public void testToString() {
    String string = runtimeTestResultEntry.toString();
    assertTrue( string.contains( severity.toString() ) );
    assertTrue( string.contains( description ) );
    assertTrue( string.contains( message ) );
    assertTrue( string.contains( String.valueOf( exception ) ) );
  }
}
