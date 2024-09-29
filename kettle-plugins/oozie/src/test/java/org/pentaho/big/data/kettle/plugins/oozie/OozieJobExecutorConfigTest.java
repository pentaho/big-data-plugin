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


package org.pentaho.big.data.kettle.plugins.oozie;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class OozieJobExecutorConfigTest {

  @Mock PropertyChangeListener listener;
  @Captor ArgumentCaptor<PropertyChangeEvent> event;

  @Before
  public void init() {
    MockitoAnnotations.initMocks( this );
  }

  @Test
  public void testAddPropertyChangeListener() throws Exception {
    OozieJobExecutorConfig config = new OozieJobExecutorConfig();

    // make sure it is capturing property change events
    config.addPropertyChangeListener( listener );
    config.setOozieWorkflow( "workflow1.xml" );

    verify( listener, times( 1 ) ).propertyChange( any( PropertyChangeEvent.class ) );
    verify( listener ).propertyChange( event.capture() );
    assertEquals( config.getOozieWorkflow(), event.getValue().getNewValue() );

    // remove the listener & verify that it isn't receiving events anymore
    config.removePropertyChangeListener( listener );
    config.setOozieWorkflow( "workflow2.xml" );
    // still 1, from the previous call
    verify( listener, times( 1 ) ).propertyChange( any( PropertyChangeEvent.class ) );
  }

  @Test
  public void testAddPropertyChangeListener_propertyName() throws Exception {
    OozieJobExecutorConfig config = new OozieJobExecutorConfig();

    // dummy property name, should not indicate any captured prop change
    config.addPropertyChangeListener( "dummy", listener );
    config.setOozieWorkflowConfig( "job0.properties" );


    verify( listener, times( 0 ) ).propertyChange( any( PropertyChangeEvent.class ) );
    // assertEquals( 0, listener.getReceivedEvents().size() );
    config.removePropertyChangeListener( "dummy", listener );

    // make sure it is capturing property change events
    config.addPropertyChangeListener( OozieJobExecutorConfig.OOZIE_WORKFLOW_CONFIG, listener );
    config.setOozieWorkflowConfig( "job1.properties" );

    verify( listener, times( 1 ) ).propertyChange( any( PropertyChangeEvent.class ) );
    verify( listener ).propertyChange( event.capture() );
    assertEquals( config.getOozieWorkflowConfig(), event.getValue().getNewValue() );

    // remove the listener & verify that it isn't receiving events anymore
    config.removePropertyChangeListener( OozieJobExecutorConfig.OOZIE_WORKFLOW_CONFIG, listener );
    config.setOozieWorkflowConfig( "job2.properties" );
    verify( listener, times( 1 ) ).propertyChange( any( PropertyChangeEvent.class ) );
  }

  @Test
  public void testGettersAndSetters() throws Exception {
    OozieJobExecutorConfig config = new OozieJobExecutorConfig();

    // everything should be null initially
    assertNull( config.getOozieUrl() );
    assertNull( config.getOozieWorkflow() );
    assertNull( config.getOozieWorkflowConfig() );

    config.setOozieUrl( "http://localhost:11000" );
    assertEquals( "http://localhost:11000", config.getOozieUrl() );

    config.setOozieWorkflow( "hdfs://localhsot:9000/user/test-user/workflowFolder" );
    assertEquals( "hdfs://localhsot:9000/user/test-user/workflowFolder", config.getOozieWorkflow() );

    config.setOozieWorkflowConfig( "job.properties" );
    assertEquals( "job.properties", config.getOozieWorkflowConfig() );

  }
}
