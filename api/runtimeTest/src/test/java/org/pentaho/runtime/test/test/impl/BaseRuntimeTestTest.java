/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.runtime.test.test.impl;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.runtime.test.result.RuntimeTestResultSummary;

import java.util.Arrays;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by bryan on 8/20/15.
 */
public class BaseRuntimeTestTest {
  private String module;
  private String id;
  private String name;
  private boolean configInitTest;
  private HashSet<String> dependencies;
  private BaseRuntimeTest baseRuntimeTest;

  @Before
  public void setup() {
    module = "module";
    id = "id";
    name = "name";
    configInitTest = true;
    dependencies = new HashSet<>( Arrays.asList( "dependency" ) );
    baseRuntimeTest = new BaseRuntimeTest( Object.class, module, id, name, configInitTest, dependencies ) {
      @Override public RuntimeTestResultSummary runTest( Object objectUnderTest ) {
        throw new UnsupportedOperationException( "This is a test object, don't run it... ever..." );
      }
    };
  }

  @Test
  public void testGetModule() {
    assertEquals( module, baseRuntimeTest.getModule() );
  }

  @Test
  public void testGetId() {
    assertEquals( id, baseRuntimeTest.getId() );
  }

  @Test
  public void testGetName() {
    assertEquals( name, baseRuntimeTest.getName() );
  }

  @Test
  public void testIsConfigInitTest() {
    assertTrue( baseRuntimeTest.isConfigInitTest() );
    assertFalse( new BaseRuntimeTest( Object.class, module, id, name, dependencies ) {
      @Override public RuntimeTestResultSummary runTest( Object objectUnderTest ) {
        throw new UnsupportedOperationException( "This is a test object, don't run it... ever..." );
      }
    }.isConfigInitTest() );
  }

  @Test
  public void testToString() {
    String string = baseRuntimeTest.toString();
    assertTrue( string.contains( module ) );
    assertTrue( string.contains( id ) );
    assertTrue( string.contains( name ) );
    assertTrue( string.contains( "true" ) );
    assertTrue( string.contains( dependencies.toString() ) );
  }
}
