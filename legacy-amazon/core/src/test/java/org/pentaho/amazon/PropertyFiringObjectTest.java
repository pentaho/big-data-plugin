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


package org.pentaho.amazon;

import org.junit.Test;
import org.pentaho.amazon.emr.job.AmazonElasticMapReduceJobExecutor;
import org.pentaho.amazon.emr.ui.AmazonElasticMapReduceJobExecutorController;
import org.pentaho.amazon.hive.job.AmazonHiveJobExecutor;
import org.pentaho.amazon.hive.ui.AmazonHiveJobExecutorController;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulEventSource;
import org.pentaho.ui.xul.binding.BindingFactory;

import java.beans.PropertyChangeEvent;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;

/**
 * This is a helper class to dynamically test a class' ability to get, set, and fire property change events for all
 * private fields.
 */
public class PropertyFiringObjectTest {

  @Test
  public void testEMRFromFieldsEvent() throws Exception {
    List<String> getterFields = new ArrayList<>();
    getterFields.add( "jarUrl" );

    AmazonElasticMapReduceJobExecutorController controller =
      new AmazonElasticMapReduceJobExecutorController( mock( XulDomContainer.class ),
        mock( AmazonElasticMapReduceJobExecutor.class ), mock( BindingFactory.class ) );

    List<Field> emrFormFields = collectChildFields( controller, getterFields );
    Class<?> oClass = controller.getClass();
    testPropertyFiringForAllPrivateFieldsOf( controller, emrFormFields, oClass );
  }

  @Test
  public void testHiveFormFieldsEvent() throws Exception {
    List<String> getterFields = new ArrayList<>();
    getterFields.add( "qUrl" );
    getterFields.add( "bootstrapActions" );

    AmazonHiveJobExecutorController controller =
      new AmazonHiveJobExecutorController( mock( XulDomContainer.class ),
        mock( AmazonHiveJobExecutor.class ), mock( BindingFactory.class ) );

    Class<?> oClass = controller.getClass();
    List<Field> hiveFormFields = collectChildFields( controller, getterFields );
    testPropertyFiringForAllPrivateFieldsOf( controller, hiveFormFields, oClass );
  }

  @Test
  public void testCommonFieldsEvent() throws Exception {
    AmazonHiveJobExecutorController controller =
      new AmazonHiveJobExecutorController( mock( XulDomContainer.class ),
        mock( AmazonHiveJobExecutor.class ), mock( BindingFactory.class ) );

    Class<?> oClass = controller.getClass().getSuperclass();
    List<Field> parentFields = collectParentFields( controller );
    testPropertyFiringForAllPrivateFieldsOf( controller, parentFields, oClass );
  }

  private List<String> getFieldNamesFormJobEntryClass() {
    List<String> fields = new ArrayList<>();
    Class<?> oClass = AbstractAmazonJobEntry.class;
    for ( Field f : oClass.getDeclaredFields() ) {
      if ( !Modifier.isProtected( f.getModifiers() ) ) {
        // Skip non-private or transient fields or final fields
        continue;
      }
      fields.add( f.getName() );
    }
    return fields;
  }

  private List<Field> collectChildFields( XulEventSource object, List<String> getterFields ) {
    List<Field> formFields = new ArrayList<>();
    Class<?> oClass = object.getClass();
    for ( Field f : oClass.getDeclaredFields() ) {
      if ( !Modifier.isPrivate( f.getModifiers() ) || Modifier.isTransient( f.getModifiers() ) ||
        Modifier.isFinal( f.getModifiers() ) || Modifier.isStatic( f.getModifiers() ) ) {
        // Skip non-private or transient fields or final fields
        continue;
      }
      if ( !getterFields.contains( f.getName() ) ) {
        continue;
      }
      formFields.add( f );
    }
    return formFields;
  }

  private List<Field> collectParentFields( XulEventSource object ) {
    List<String> jobEnrtyFields = getFieldNamesFormJobEntryClass();
    List<Field> formFields = new ArrayList<>();
    Class<?> oClass = object.getClass().getSuperclass();
    for ( Field f : oClass.getDeclaredFields() ) {
      if ( !Modifier.isProtected( f.getModifiers() ) || Modifier.isTransient( f.getModifiers() ) ||
        Modifier.isFinal( f.getModifiers() ) || Modifier.isStatic( f.getModifiers() ) ) {
        // Skip non-private or transient fields or final fields
        continue;
      }
      if ( !jobEnrtyFields.contains( f.getName() ) ) {
        continue;
      }
      formFields.add( f );
    }
    return formFields;
  }

  /**
   * Test that all private fields have getter and setter methods, and they work as they should: the getter returns the
   * value and the setter generates a {@link PropertyChangeEvent} for that property.
   *
   * @param object instance of event source to test
   * @param fields list of form fields
   * @param oClass JobEntry class
   * @throws Exception
   */
  private void testPropertyFiringForAllPrivateFieldsOf( XulEventSource object, List<Field> fields, Class<?> oClass )
    throws Exception {
    // Attach our property change listener to the object
    PersistentPropertyChangeListener listner = new PersistentPropertyChangeListener();
    object.addPropertyChangeListener( listner );

    try {
      for ( Field f : fields ) {
        String fullFieldName = oClass.getSimpleName() + "." + f.getName();
        try {
          // Clear out the previous run's events if there were any
          listner.getReceivedEvents().clear();
          String camelcaseFieldName = f.getName().substring( 0, 1 ).toUpperCase() + f.getName().substring( 1 );

          // Grab the getter and setter methods for the field
          Method getter = findMethod( oClass, camelcaseFieldName, null, "get", "is" );
          Method setter = findMethod( oClass, camelcaseFieldName, new Class<?>[] { f.getType() }, "set" );

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
   * Finds a method in the given class or any super class with the name {@code prefix + methodName} that accepts 0
   * parameters.
   *
   * @param aClass         Class to search for method in
   * @param methodName     Camelcase'd method name to search for with any of the provided prefixes
   * @param parameterTypes The parameter types the method signature must match.
   * @param prefixes       Prefixes to prepend to {@code methodName} when searching for method names, e.g. "get", "is"
   * @return The first method found to match the format {@code prefix + methodName}
   */
  private static Method findMethod( Class<?> aClass, String methodName, Class<?>[] parameterTypes,
                                    String... prefixes ) {
    for ( String prefix : prefixes ) {
      try {
        return aClass.getDeclaredMethod( prefix + methodName, parameterTypes );
      } catch ( NoSuchMethodException ex ) {
        // ignore, continue searching prefixes
      }
    }
    // If no method found with any prefixes search the super class
    aClass = aClass.getSuperclass();
    return aClass == null ? null : findMethod( aClass, methodName, parameterTypes, prefixes );
  }


  /**
   * Get a value to test with that matches the type provided.
   *
   * @param type          Type of test value
   * @param originalValue The test value returned by this method must differ from this object
   * @return An object of type {@code Type}
   */

  private Object getTestValue( Class<?> type, Object originalValue ) {
    if ( String.class.equals( type ) ) {
      return String.valueOf( System.currentTimeMillis() );
    }
    if ( Boolean.class.equals( type ) || boolean.class.equals( type ) ) {
      // Return the opposite
      return originalValue == null ? Boolean.TRUE : !(Boolean) originalValue;
    }
    //not primitive
    return mock( type );
  }
}
