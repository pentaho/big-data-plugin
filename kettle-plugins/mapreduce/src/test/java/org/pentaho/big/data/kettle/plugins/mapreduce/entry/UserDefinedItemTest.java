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

package org.pentaho.big.data.kettle.plugins.mapreduce.entry;

import org.junit.Before;
import org.junit.Test;

import java.beans.PropertyChangeListener;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * Created by bryan on 1/14/16.
 */
public class UserDefinedItemTest {
  private UserDefinedItem userDefinedItem;

  @Before
  public void setup() {
    userDefinedItem = new UserDefinedItem();
  }

  @Test
  public void testGetSetName() {
    String testName = "testName";
    userDefinedItem.setName( testName );
    assertEquals( testName, userDefinedItem.getName() );
  }

  @Test
  public void testGetSetValue() {
    String testValue = "testValue";
    userDefinedItem.setValue( testValue );
    assertEquals( testValue, userDefinedItem.getValue() );
  }

  @Test
  public void testAddRemovePropertyChangeListener() {
    PropertyChangeListener propertyChangeListener = mock( PropertyChangeListener.class );
    userDefinedItem.addPropertyChangeListener( propertyChangeListener );
    userDefinedItem.setName( "test" );
    userDefinedItem.setValue( "test2" );
    userDefinedItem.removePropertyChangeListener( propertyChangeListener );
    verifyNoMoreInteractions( propertyChangeListener );
  }
}
