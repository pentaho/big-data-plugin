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
