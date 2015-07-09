/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.ui.job.entries.sqoop;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DatabaseItemTest {

  @Test
  public void instantiate_name() {
    String name = "name";
    DatabaseItem item = new DatabaseItem( name );

    assertEquals( name, item.getName() );
    assertEquals( name, item.getDisplayName() );
  }

  @Test
  public void instantiate_displayName() {
    String name = "name";
    String displayName = "display name";
    DatabaseItem item = new DatabaseItem( name, displayName );

    assertEquals( name, item.getName() );
    assertEquals( displayName, item.getDisplayName() );
  }

  @Test
  public void equals() {
    DatabaseItem item1 = new DatabaseItem( "test" );
    DatabaseItem item2 = new DatabaseItem( "test" );
    DatabaseItem item3 = new DatabaseItem( "testing" );

    assertFalse( item1.equals( null ) );
    assertFalse( item1.equals( item3 ) );

    assertTrue( item1.equals( item1 ) );
    assertTrue( item1.equals( item2 ) );
  }

  @Test
  public void testHashCode() {
    String name = "test";
    DatabaseItem item1 = new DatabaseItem( name );

    assertEquals( name.hashCode(), item1.hashCode() );
  }

  @Test
  public void testToString() {
    String name = "test";
    DatabaseItem item = new DatabaseItem( name );

    assertEquals( name, item.toString() );
  }
}
