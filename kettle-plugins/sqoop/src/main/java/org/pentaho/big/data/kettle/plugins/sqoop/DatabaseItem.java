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

/**
 * A database item represents a named database. It is used to display existing databases in a menu or combo box.
 */
public class DatabaseItem {
  private String name;
  private String displayName;

  public DatabaseItem( String name ) {
    this( name, name );
  }

  public DatabaseItem( String name, String displayName ) {
    this.name = name;
    this.displayName = displayName;
  }

  public String getName() {
    return name;
  }

  public String getDisplayName() {
    return displayName;
  }

  /**
   * This is what will be visible in the menu/combo box.
   * 
   * @return the name of this database item
   */
  @Override
  public String toString() {
    return getDisplayName();
  }

  @Override
  public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    DatabaseItem that = (DatabaseItem) o;

    if ( name != null ? !name.equals( that.name ) : that.name != null ) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return name != null ? name.hashCode() : 0;
  }
}
