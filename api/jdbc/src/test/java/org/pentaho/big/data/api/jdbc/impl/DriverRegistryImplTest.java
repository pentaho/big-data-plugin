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
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.sql.Driver;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 4/29/16.
 */
@RunWith( MockitoJUnitRunner.class )
public class DriverRegistryImplTest {
  @Mock Driver driver;
  String testURL;
  DriverRegistryImpl driverRegistry;

  @Before
  public void setup() throws SQLException {
    driverRegistry = new DriverRegistryImpl();
    driverRegistry.registerDriver( driver );
    testURL = "testURL";
  }

  @Test
  public void testRegisterGetDrivers() throws SQLException {
    assertEquals( 1, driverRegistry.getDrivers().size() );
    assertEquals( driver, driverRegistry.getDrivers().get( 0 ) );
  }

  @Test
  public void testDeregisterGetDrivers() throws SQLException {
    driverRegistry.deregisterDriver( driver );
    assertEquals( 0, driverRegistry.getDrivers().size() );
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
