/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

package org.pentaho.big.data.api.jdbc.impl;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceEvent;
import org.osgi.framework.ServiceListener;
import org.osgi.framework.ServiceReference;

import java.sql.Driver;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 4/29/16.
 */
@RunWith( MockitoJUnitRunner.class )
public class DriverLocatorImplTest {
  @Mock BundleContext bundleContext;
  @Mock HasRegisterDriver hasRegisterDriver;
  @Mock HasDeregisterDriver hasDeregisterDriver;
  @Mock Driver driver;
  @Mock ServiceReference<Driver> serviceReference;
  Map<ServiceReference<Driver>, List<Driver>> registeredDrivers;
  ServiceListener serviceListener;
  String testURL;
  DriverLocatorImpl driverRegistry;

  @Before
  public void setup() throws SQLException, InvalidSyntaxException {
    registeredDrivers = new HashMap<>();
    driverRegistry = new DriverLocatorImpl( bundleContext, hasRegisterDriver, hasDeregisterDriver, registeredDrivers );
    ArgumentCaptor<ServiceListener> serviceListenerArgumentCaptor = ArgumentCaptor.forClass( ServiceListener.class );
    verify( bundleContext ).addServiceListener( serviceListenerArgumentCaptor.capture() );
    serviceListener = serviceListenerArgumentCaptor.getValue();
    when( bundleContext.getServiceReferences( Driver.class, DriverLocatorImpl.DATA_SOURCE_TYPE_BIGDATA ) )
      .thenReturn( Arrays.asList( serviceReference ) );
    when( bundleContext.getService( serviceReference ) ).thenReturn( driver );
    testURL = "testURL";
  }

  @Test
  public void testGetDrivers() throws SQLException {
    List<Map.Entry<ServiceReference<Driver>, Driver>> driverList = new ArrayList<>();
    driverRegistry.getDrivers().forEachRemaining( driverList::add );
    assertEquals( 1, driverList.size() );
    assertEquals( serviceReference, driverList.get( 0 ).getKey() );
    assertEquals( driver, driverList.get( 0 ).getValue() );
  }

  @Test
  public void testGetDriversError() throws InvalidSyntaxException {
    bundleContext = mock( BundleContext.class );
    when( bundleContext.getServiceReferences( Driver.class, null ) ).thenThrow( new InvalidSyntaxException( "", "" ) );
    assertFalse( new DriverLocatorImpl( bundleContext ).getDrivers().hasNext() );
  }

  @Test
  public void testDeregisterGetDrivers() throws SQLException {
    ServiceEvent serviceEvent = mock( ServiceEvent.class );
    driverRegistry.registerDriverServiceReferencePair( serviceReference, driver, true );
    verify( hasRegisterDriver ).registerDriver( driver );
    assertEquals( 1, registeredDrivers.size() );
    when( serviceEvent.getServiceReference() ).thenReturn( (ServiceReference) serviceReference );
    serviceListener.serviceChanged( serviceEvent );
    assertEquals( 0, registeredDrivers.size() );
  }

  @Test
  public void testDeregisterError() throws SQLException {
    ServiceEvent serviceEvent = mock( ServiceEvent.class );
    when( serviceEvent.getServiceReference() ).thenReturn( (ServiceReference) serviceReference );
    driverRegistry.registerDriverServiceReferencePair( serviceReference, driver, false );
    doThrow( new SQLException() ).when( hasDeregisterDriver ).deregisterDriver( driver );
    serviceListener.serviceChanged( serviceEvent );
    verify( hasDeregisterDriver ).deregisterDriver( driver );
  }

  @Test( expected = UnsupportedOperationException.class )
  public void testSetValueThrowsUnsupported() {
    driverRegistry.registerDriverServiceReferencePair( serviceReference, driver, false );
    driverRegistry.getDrivers().next().setValue( null );
  }

  @Test
  public void testRegisterMultiple() {
    Driver driver2 = mock( Driver.class );
    driverRegistry.registerDriverServiceReferencePair( serviceReference, driver, false );
    driverRegistry.registerDriverServiceReferencePair( serviceReference, driver2, false );
    assertEquals( 1, registeredDrivers.size() );
    List<Driver> drivers = registeredDrivers.get( serviceReference );
    assertEquals( 2, drivers.size() );
    assertEquals( driver, drivers.get( 0 ) );
    assertEquals( driver2, drivers.get( 1 ) );
  }

  @Test
  public void testRegisterException() throws SQLException {
    doThrow( new SQLException() ).when( hasRegisterDriver ).registerDriver( driver );
    driverRegistry.registerDriverServiceReferencePair( serviceReference, driver, true );
    assertEquals( 0, registeredDrivers.size() );
  }

  @Test
  public void testGetDriver() throws SQLException {
    when( driver.acceptsURL( testURL ) ).thenReturn( true );
    assertEquals( driver, driverRegistry.getDriver( testURL ) );
  }

  @Test
  public void testGetDriverNull() throws SQLException {
    when( driver.acceptsURL( testURL ) ).thenReturn( false );
    assertNull( driverRegistry.getDriver( testURL ) );
  }

  @Test
  public void testGetDriverException() throws SQLException {
    when( driver.acceptsURL( testURL ) ).thenThrow( new SQLException() );
    assertNull( driverRegistry.getDriver( testURL ) );
  }
}
