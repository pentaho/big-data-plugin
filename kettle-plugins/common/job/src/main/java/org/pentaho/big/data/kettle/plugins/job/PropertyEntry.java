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


package org.pentaho.big.data.kettle.plugins.job;

import org.pentaho.ui.xul.XulEventSource;

import java.beans.PropertyChangeListener;
import java.util.Map;

/**
 * User: RFellows Date: 6/18/12
 */
public class PropertyEntry implements Map.Entry<String, String>, XulEventSource {
  private String key = null;
  private String value = null;

  public PropertyEntry() {
    this( null, null );
  }

  public PropertyEntry( String key, String value ) {
    this.key = key;
    this.value = value;
  }

  @Override
  public String getKey() {
    return key;
  }

  public void setKey( String key ) {
    this.key = key;
  }

  @Override
  public String getValue() {
    return value;
  }

  @Override
  public String setValue( String value ) {
    this.value = value;
    return value;
  }

  @Override
  public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }

    PropertyEntry that = (PropertyEntry) o;

    if ( key != null ? !key.equals( that.key ) : that.key != null ) {
      return false;
    }
    if ( value != null ? !value.equals( that.value ) : that.value != null ) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result = key != null ? key.hashCode() : 0;
    result = 31 * result + ( value != null ? value.hashCode() : 0 );
    return result;
  }

  @Override
  public void addPropertyChangeListener( PropertyChangeListener propertyChangeListener ) {
  }

  @Override
  public void removePropertyChangeListener( PropertyChangeListener propertyChangeListener ) {
  }
}
