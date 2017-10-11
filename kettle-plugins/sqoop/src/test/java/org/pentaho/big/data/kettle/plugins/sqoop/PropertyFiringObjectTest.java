/*! ******************************************************************************
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

package org.pentaho.big.data.kettle.plugins.sqoop;

import org.junit.Test;
import org.pentaho.ui.xul.XulEventSource;

import java.beans.PropertyChangeEvent;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;

/**
 * This is a helper class to dynamically test a class' ability to get, set, and fire property change events for all
 * private fields.
 */
public class PropertyFiringObjectTest {

  @Test
  public void testSqoopExportConfig() throws Exception {
    testPropertyFiringForAllPrivateFieldsOf( new SqoopExportConfig( mock( SqoopExportJobEntry.class ) ) );
  }

  @Test
  public void testSqoopImportConfig() throws Exception {
    testPropertyFiringForAllPrivateFieldsOf( new SqoopImportConfig( mock( SqoopImportJobEntry.class ) ) );
  }

  /**
   * Test that all private fields have getter and setter methods, and they work as they should: the getter returns the
   * value and the setter generates a {@link PropertyChangeEvent} for that property.
   *
   * @param o
   *          instance of event source to test
   * @throws Exception
   */
  private void testPropertyFiringForAllPrivateFieldsOf( XulEventSource object ) throws Exception {
    // Attach our property change listener to the object
    PersistentPropertyChangeListener listner = new PersistentPropertyChangeListener();
    object.addPropertyChangeListener( listner );
    try {
      Class<?> oClass = object.getClass();
      for ( Field f : oClass.getDeclaredFields() ) {
        if ( !Modifier.isPrivate( f.getModifiers() ) || Modifier.isTransient( f.getModifiers() ) ||
            Modifier.isFinal( f.getModifiers() )) {
          // Skip non-private or transient fields or final fields
          continue;
        }

        String fullFieldName = object.getClass().getSimpleName() + "." + f.getName();
        try {
          // Clear out the previous run's events if there were any
          listner.getReceivedEvents().clear();

          String camelcaseFieldName = f.getName().substring( 0, 1 ).toUpperCase() + f.getName().substring( 1 );

          // Grab the getter and setter methods for the field
          Method getter = SqoopUtils.findMethod( oClass, camelcaseFieldName, null, "get", "is" );
          Method setter = SqoopUtils.findMethod( oClass, camelcaseFieldName, new Class<?>[] { f.getType() }, "set" );

          // Grab the original value so we can make sure we're changing it so guarantee a PropertyChangeEvent
          Object originalValue = getter.invoke( object );
          // Generate a test value to set this property to
          Object value = getTestValue( f.getType(), originalValue );

          assertFalse( fullFieldName + ": generated value does not differ from original value. Please update "
              + getClass() + ".getTestValue() to return a different value for " + f.getType() + ".", value.equals(
                  originalValue ) );
          setter.invoke( object, value );
          assertEquals( fullFieldName + ": value not get/set properly", value, getter.invoke( object ) );
          assertEquals( fullFieldName + ": PropertyChangeEvent not received when changing value", 1, listner
              .getReceivedEvents().size() );
          PropertyChangeEvent evt = listner.getReceivedEvents().get( 0 );
          assertEquals( fullFieldName + ": fired event with wrong property name", f.getName(), evt.getPropertyName() );
          assertEquals( fullFieldName + ": fired event with incorrect old value", originalValue, evt.getOldValue() );
          assertEquals( fullFieldName + ": fired event with incorrect new value", value, evt.getNewValue() );
        } catch ( Exception ex ) {
          throw new Exception( "Error testing getter/setter for " + fullFieldName, ex );
        }
      }
    } finally {
      // Remove our listener when we're done
      object.removePropertyChangeListener( listner );
    }
  }

  /**
   * Get a value to test with that matches the type provided.
   *
   * @param type
   *          Type of test value
   * @param originalValue
   *          The test value returned by this method must differ from this object
   * @return An object of type {@code Type}
   */

  private Object getTestValue( Class<?> type, Object originalValue ) {
    if ( String.class.equals( type ) ) {
      return  String.valueOf( System.currentTimeMillis() );
    }
    if ( Boolean.class.equals( type ) ) {
      // Return the opposite
      return originalValue == null ? Boolean.TRUE : !(Boolean) originalValue;
    }
    //not primitive
    return mock( type );
  }
}
