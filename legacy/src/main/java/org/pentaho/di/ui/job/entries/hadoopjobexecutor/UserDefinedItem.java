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


package org.pentaho.di.ui.job.entries.hadoopjobexecutor;

import java.beans.PropertyChangeListener;

import org.pentaho.ui.xul.XulEventSource;

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
