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
