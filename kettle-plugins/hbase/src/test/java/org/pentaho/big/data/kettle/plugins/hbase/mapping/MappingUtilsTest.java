/*! ******************************************************************************
 *
 * Pentaho Data Integration
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
package org.pentaho.big.data.kettle.plugins.hbase.mapping;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import static org.mockito.Mockito.verify;

import org.junit.Test;
import org.pentaho.big.data.api.initializer.ClusterInitializationException;
import org.pentaho.big.data.kettle.plugins.hbase.HBaseConnectionException;
import org.pentaho.bigdata.api.hbase.HBaseConnection;

/**
 * @author Tatsiana_Kasiankova
 *
 */
public class MappingUtilsTest {

  /**
   *
   */
  private static final String UNABLE_TO_CONNECT_TO_H_BASE = "Unable to connect to HBase";
  private ConfigurationProducer cProducerMock = mock( ConfigurationProducer.class );
  private HBaseConnection hbConnectionMock = mock( HBaseConnection.class );

  @Test
  public void testGetMappingAdmin_NoException() {
    try {
      when( cProducerMock.getHBaseConnection() ).thenReturn( hbConnectionMock );
      MappingAdmin mappingAdmin = MappingUtils.getMappingAdmin( cProducerMock );
      assertNotNull( mappingAdmin );
      assertSame( hbConnectionMock, mappingAdmin.getConnection() );
      verify( hbConnectionMock ).checkHBaseAvailable();
    } catch ( Exception e ) {
      fail( "No exception expected but it occurs!" );
    }
  }

  @Test
  public void testGetMappingAdmin_ClusterInitializationExceptionToHBaseConnectionException() throws Exception {
    ClusterInitializationException clusterInitializationException =
        new ClusterInitializationException( new Exception( "ClusterInitializationException" ) );
    try {
      when( cProducerMock.getHBaseConnection() ).thenThrow( clusterInitializationException );
      MappingUtils.getMappingAdmin( cProducerMock );
      fail( "Expected HBaseConnectionException but it doen not occur!" );
    } catch ( HBaseConnectionException e ) {
      assertEquals( UNABLE_TO_CONNECT_TO_H_BASE, e.getMessage() );
      assertSame( clusterInitializationException, e.getCause() );
    }
  }

  @Test
  public void testGetMappingAdmin_IOExceptionToHBaseConnectionException() throws Exception {
    IOException ioException = new IOException( "IOException" );
    try {
      when( cProducerMock.getHBaseConnection() ).thenThrow( ioException );
      MappingUtils.getMappingAdmin( cProducerMock );
      fail( "Expected HBaseConnectionException but it doen not occur!" );
    } catch ( HBaseConnectionException e ) {
      assertEquals( UNABLE_TO_CONNECT_TO_H_BASE, e.getMessage() );
      assertSame( ioException, e.getCause() );
    }
  }

}
