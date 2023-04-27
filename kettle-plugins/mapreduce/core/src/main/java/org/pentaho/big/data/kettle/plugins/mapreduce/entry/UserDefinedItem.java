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


package org.pentaho.big.data.kettle.plugins.mapreduce.entry;

import org.pentaho.ui.xul.XulEventSource;

import java.beans.PropertyChangeListener;

public class UserDefinedItem implements XulEventSource {
  private String name;
  private String value;

  public UserDefinedItem() {
  }

  public String getName() {
    return name;
  }

  public void setName( String name ) {
    this.name = name;
  }

  public String getValue() {
    return value;
  }

  public void setValue( String value ) {
    this.value = value;
  }

  public void addPropertyChangeListener( PropertyChangeListener listener ) {
  }

  public void removePropertyChangeListener( PropertyChangeListener listener ) {
  }

}
