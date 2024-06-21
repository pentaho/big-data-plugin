/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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
import static org.mockito.ArgumentMatchers.any;
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
