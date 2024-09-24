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
import java.beans.PropertyChangeSupport;

/**
 * User: RFellows Date: 6/5/12
 */
public class BlockableJobConfig implements XulEventSource, Cloneable {
  protected transient PropertyChangeSupport pcs = new PropertyChangeSupport( this );
  protected String jobEntryName = null;
  protected String blockingPollingInterval = String.valueOf( 300 );
  protected String blockingExecution = Boolean.TRUE.toString();

  public static final String JOB_ENTRY_NAME = "jobEntryName";
  public static final String BLOCKING_EXECUTION = "blockingExecution";
  public static final String BLOCKING_POLLING_INTERVAL = "blockingPollingInterval";

  public String getJobEntryName() {
    return jobEntryName;
  }

  public void setJobEntryName( String jobEntryName ) {
    String old = this.jobEntryName;
    this.jobEntryName = jobEntryName;
    pcs.firePropertyChange( JOB_ENTRY_NAME, old, this.jobEntryName );
  }

  public String getBlockingPollingInterval() {
    return blockingPollingInterval;
  }

  public void setBlockingPollingInterval( String blockingPollingInterval ) {
    String old = this.blockingPollingInterval;
    this.blockingPollingInterval = blockingPollingInterval;
    pcs.firePropertyChange( BLOCKING_POLLING_INTERVAL, old, this.blockingPollingInterval );
  }

  public String getBlockingExecution() {
    return blockingExecution;
  }

  public void setBlockingExecution( String blockingExecution ) {
    String old = this.blockingExecution;
    this.blockingExecution = blockingExecution;
    pcs.firePropertyChange( BLOCKING_EXECUTION, old, this.blockingExecution );
  }

  /**
   * @see {@link PropertyChangeSupport#addPropertyChangeListener(PropertyChangeListener)}
   */
  public void addPropertyChangeListener( PropertyChangeListener l ) {
    pcs.addPropertyChangeListener( l );
  }

  /**
   * @see {@link PropertyChangeSupport#addPropertyChangeListener(String, PropertyChangeListener)}
   */
  public void addPropertyChangeListener( String propertyName, PropertyChangeListener l ) {
    pcs.addPropertyChangeListener( propertyName, l );
  }

  /**
   * @see {@link PropertyChangeSupport#removePropertyChangeListener(PropertyChangeListener)}
   */
  public void removePropertyChangeListener( PropertyChangeListener l ) {
    pcs.removePropertyChangeListener( l );
  }

  /**
   * @see {@link PropertyChangeSupport#removePropertyChangeListener(String, PropertyChangeListener)}
   */
  public void removePropertyChangeListener( String propertyName, PropertyChangeListener l ) {
    pcs.removePropertyChangeListener( propertyName, l );
  }

  @Override
  public Object clone() {
    try {
      return super.clone();
    } catch ( CloneNotSupportedException e ) {
      throw new RuntimeException( e );
    }
  }

  @Override
  public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    BlockableJobConfig that = (BlockableJobConfig) o;

    if ( blockingExecution != null ? !blockingExecution.equals( that.blockingExecution )
        : that.blockingExecution != null ) {
      return false;
    }
    if ( blockingPollingInterval != null ? !blockingPollingInterval.equals( that.blockingPollingInterval )
        : that.blockingPollingInterval != null ) {
      return false;
    }
    if ( jobEntryName != null ? !jobEntryName.equals( that.jobEntryName ) : that.jobEntryName != null ) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result = jobEntryName != null ? jobEntryName.hashCode() : 0;
    result = 31 * result + ( blockingPollingInterval != null ? blockingPollingInterval.hashCode() : 0 );
    result = 31 * result + ( blockingExecution != null ? blockingExecution.hashCode() : 0 );
    return result;
  }
}
