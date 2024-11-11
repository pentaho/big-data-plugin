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
import org.pentaho.runtime.test.RuntimeTest;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 8/20/15.
 */
public class RuntimeTestDelegateWithMoreDependenciesTest {
  private RuntimeTest delegate;
  private HashSet<String> extraDependencies;
  private RuntimeTestDelegateWithMoreDependencies
  runtimeTestDelegateWithMoreDependencies;
  private String module;
  private String id;
  private String name;
  private String inheritedDep;
  private String newDep;

  @Before
  public void setup() {
    delegate = mock( RuntimeTest.class );
    module = "module";
    id = "id";
    name = "name";
    inheritedDep = "inheritedDep";
    newDep = "newDep";
    when( delegate.getModule() ).thenReturn( module );
    when( delegate.getId() ).thenReturn( id );
    when( delegate.getName() ).thenReturn( name );
    when( delegate.getDependencies() ).thenReturn( new HashSet<>( Arrays.asList( inheritedDep ) ) );
    extraDependencies = new HashSet<>( Arrays.asList( newDep ) );
    runtimeTestDelegateWithMoreDependencies =
      new RuntimeTestDelegateWithMoreDependencies( delegate, extraDependencies );
  }

  @Test
  public void testGetModule() {
    assertEquals( module, runtimeTestDelegateWithMoreDependencies.getModule() );
  }

  @Test
  public void testGetId() {
    assertEquals( id, runtimeTestDelegateWithMoreDependencies.getId() );
  }

  @Test
  public void testGetName() {
    assertEquals( name, runtimeTestDelegateWithMoreDependencies.getName() );
  }

  @Test
  public void testIsConfigInitTest() {
    when( delegate.isConfigInitTest() ).thenReturn( false ).thenReturn( true );
    assertFalse( runtimeTestDelegateWithMoreDependencies.isConfigInitTest() );
    assertTrue( runtimeTestDelegateWithMoreDependencies.isConfigInitTest() );
  }

  @Test
  public void testGetDependencies() {
    Set<String> dependencies = runtimeTestDelegateWithMoreDependencies.getDependencies();
    assertTrue( dependencies.contains( inheritedDep ) );
    assertTrue( dependencies.contains( newDep ) );
  }

  @Test
  public void testToString() {
    String string = runtimeTestDelegateWithMoreDependencies.toString();
    assertTrue( string.contains( delegate.toString() ) );
    assertTrue( string.contains( newDep ) );
  }
}
