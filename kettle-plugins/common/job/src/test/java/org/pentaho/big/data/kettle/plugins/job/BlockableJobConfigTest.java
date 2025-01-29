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


package org.pentaho.big.data.kettle.plugins.job;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;

import static junit.framework.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * User: RFellows Date: 6/5/12
 */
public class BlockableJobConfigTest {

  @Mock PropertyChangeListener listener;
  @Captor ArgumentCaptor<PropertyChangeEvent> event;

  @Before
  public void init() {
    MockitoAnnotations.initMocks( this );
  }


  @Test
  public void testAddPropertyChangeListener() throws Exception {
    BlockableJobConfig config = new BlockableJobConfig();

    // make sure it is capturing property change events
    config.addPropertyChangeListener( listener );
    config.setJobEntryName( "jobName1" );
    verify( listener, times( 1 ) ).propertyChange( any( PropertyChangeEvent.class ) );
    verify( listener ).propertyChange( event.capture() );
    assertEquals( config.getJobEntryName(), event.getValue().getNewValue() );

    // remove the listener & verify that it isn't receiving events anymore
    config.removePropertyChangeListener( listener );
    config.setJobEntryName( "jobName2" );
    verify( listener, times( 1 ) ).propertyChange( any( PropertyChangeEvent.class ) ); // still 1, from the previous call
  }

  @Test
  public void testAddPropertyChangeListener_propertyName() throws Exception {
    BlockableJobConfig config = new BlockableJobConfig();

    // dummy property name, should not indicate any captured prop change
    config.addPropertyChangeListener( "dummy", listener );
    config.setJobEntryName( "jobName0" );
    verify( listener, times( 0 ) ).propertyChange( any( PropertyChangeEvent.class ) );
    config.removePropertyChangeListener( "dummy", listener );

    // make sure it is capturing property change events
    config.addPropertyChangeListener( BlockableJobConfig.JOB_ENTRY_NAME, listener );
    config.setJobEntryName( "jobName1" );
    verify( listener, times( 1 ) ).propertyChange( any( PropertyChangeEvent.class ) );
    verify( listener ).propertyChange( event.capture() );
    assertEquals( config.getJobEntryName(), event.getValue().getNewValue() );

    // remove the listener & verify that it isn't receiving events anymore
    config.removePropertyChangeListener( BlockableJobConfig.JOB_ENTRY_NAME, listener );
    config.setJobEntryName( "jobName2" );
    verify( listener, times( 1 ) ).propertyChange( any( PropertyChangeEvent.class ) ); // still 1, from the previous call
  }

  @Test
  public void testGetterAndSetter() throws Exception {
    BlockableJobConfig config = new BlockableJobConfig();
    assertNull( config.getJobEntryName() );

    config.setJobEntryName( "jobName" );
    assertEquals( "jobName", config.getJobEntryName() );
  }

  @Test
  public void testClone() throws Exception {
    BlockableJobConfig configOrig = new BlockableJobConfig();
    configOrig.setJobEntryName( "Test" );
    BlockableJobConfig configCloned = (BlockableJobConfig) configOrig.clone();

    assertNotSame( configOrig, configCloned );
    assertEquals( configOrig, configCloned );

    configOrig.setJobEntryName( "New Name" );
    assertFalse( configOrig.getJobEntryName().equals( configCloned.getJobEntryName() ) );

  }
}
