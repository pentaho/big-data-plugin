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


package org.pentaho.big.data.kettle.plugins.sqoop;

import org.pentaho.ui.xul.XulEventSource;

import java.beans.PropertyChangeListener;
import java.lang.reflect.Method;

/**
 * Represents a command line argument. This is required to display an argument in a list of arguments for the UI.
 */
public class ArgumentWrapper implements XulEventSource {
  private String name;
  private String displayName;
  private boolean flag;
  private String prefix;
  private int order;

  private Object target;
  private Method getter;
  private Method setter;

  public ArgumentWrapper( String name, String displayName,
      boolean flag, String prefix, int order,
      Object target, Method getter, Method setter ) {
    if ( name == null || target == null || getter == null || setter == null ) {
      throw new NullPointerException();
    }
    validateAccessors( getter, setter );

    this.name = name;
    this.displayName = displayName;
    this.flag = flag;
    this.prefix = prefix;
    this.order = order;
    this.target = target;
    this.getter = getter;
    this.setter = setter;
  }

  private void validateAccessors( Method getter, Method setter ) {
    if ( getter.getReturnType() != String.class ) {
      throw new IllegalArgumentException( "Invalid getter method. Method must return a String," );
    }
    if ( setter.getParameterTypes().length < 1 || setter.getParameterTypes()[0] != String.class ) {
      throw new IllegalArgumentException( "Invalid setter method. Method must accept a single String parameter." );
    }
  }

  public String getName() {
    return name;
  }

  public void setName( String name ) {
    this.name = name;
  }

  public String getDisplayName() {
    return displayName;
  }

  public void setDisplayName( String displayName ) {
    this.displayName = displayName;
  }

  public void setValue( String value ) {
    try {
      if ( "".equals( value ) ) {
        value = null;
      }

      setter.invoke( target, value );
    } catch ( Exception ex ) {
      throw new RuntimeException( "error setting value for argument " + getName(), ex );
    }
  }

  public String getValue() {
    try {
      return String.class.cast( getter.invoke( target ) );
    } catch ( Exception ex ) {
      throw new RuntimeException( "error retrieving value for argument " + getName(), ex );
    }
  }

  public boolean isFlag() {
    return flag;
  }

  public void setFlag( boolean flag ) {
    this.flag = flag;
  }

  /**
   * Uses the argument's name to determine equality.
   * 
   * @param o
   *          another argument
   * @return {@code true} if {@code o} is an {@link ArgumentWrapper} and its name equals this argument's name
   */
  @Override
  public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }

    ArgumentWrapper that = (ArgumentWrapper) o;

    return this.name.equals( that.name );
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Override
  public void addPropertyChangeListener( PropertyChangeListener listener ) {
    // Do nothing, this object is a wrapper and firing events here propagates to too many objects
  }

  @Override
  public void removePropertyChangeListener( PropertyChangeListener listener ) {
    // Do nothing, this object is a wrapper and firing events here propagates to too many objects
  }

  public String getPrefix() {
    return prefix;
  }

  public void setPrefix( String prefix ) {
    this.prefix = prefix;
  }

  public int getOrder() {
    return order;
  }

  public void setOrder( int order ) {
    this.order = order;
  }
}
