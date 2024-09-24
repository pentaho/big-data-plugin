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

package org.pentaho.amazon;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.ArrayList;
import java.util.List;

/**
 * Property Change Listener that records all received events, useful for test purposes.
 */
public class PersistentPropertyChangeListener implements PropertyChangeListener {
  private List<PropertyChangeEvent> receivedEvents;

  public PersistentPropertyChangeListener() {
    receivedEvents = new ArrayList<>();
  }

  @Override
  public void propertyChange( PropertyChangeEvent evt ) {
    receivedEvents.add( evt );
  }

  /**
   * @return every event received by this listener
   */
  public List<PropertyChangeEvent> getReceivedEvents() {
    return receivedEvents;
  }

  /**
   * @return only the events that resulted in changed values
   */
  public List<PropertyChangeEvent> getReceivedEventsWithChanges() {
    List<PropertyChangeEvent> events = new ArrayList<PropertyChangeEvent>();
    for ( PropertyChangeEvent evt : receivedEvents ) {
      if ( !( evt.getOldValue() == null ? evt.getNewValue() == null :
        evt.getOldValue().equals( evt.getNewValue() ) ) ) {
        events.add( evt );
      }
    }
    return events;
  }
}
