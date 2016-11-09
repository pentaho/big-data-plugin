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

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
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
  private ConfigurationProducer cProducerMock = Mockito.mock( ConfigurationProducer.class );
  private HBaseConnection hbConnectionMock = Mockito.mock( HBaseConnection.class );

  @Test
  public void testGetMappingAdmin_NoException() {
    try {
      Mockito.when( cProducerMock.getHBaseConnection() ).thenReturn( hbConnectionMock );
      MappingAdmin mappingAdmin = MappingUtils.getMappingAdmin( cProducerMock );
      Assert.assertNotNull( mappingAdmin );
      Assert.assertSame( hbConnectionMock, mappingAdmin.getConnection() );
      Mockito.verify( hbConnectionMock ).checkHBaseAvailable();
    } catch ( Exception e ) {
      Assert.fail( "No exception expected but it occurs!" );
    }
  }

  @Test
  public void testGetMappingAdmin_ClusterInitializationExceptionToHBaseConnectionException() throws Exception {
    ClusterInitializationException clusterInitializationException =
        new ClusterInitializationException( new Exception( "ClusterInitializationException" ) );
    try {
      Mockito.when( cProducerMock.getHBaseConnection() ).thenThrow( clusterInitializationException );
      MappingUtils.getMappingAdmin( cProducerMock );
      Assert.fail( "Expected HBaseConnectionException but it doen not occur!" );
    } catch ( HBaseConnectionException e ) {
      Assert.assertEquals( UNABLE_TO_CONNECT_TO_H_BASE, e.getMessage() );
      Assert.assertSame( clusterInitializationException, e.getCause() );
    }
  }

  @Test
  public void testGetMappingAdmin_IOExceptionToHBaseConnectionException() throws Exception {
    IOException ioException = new IOException( "IOException" );
    try {
      Mockito.when( cProducerMock.getHBaseConnection() ).thenThrow( ioException );
      MappingUtils.getMappingAdmin( cProducerMock );
      Assert.fail( "Expected HBaseConnectionException but it doen not occur!" );
    } catch ( HBaseConnectionException e ) {
      Assert.assertEquals( UNABLE_TO_CONNECT_TO_H_BASE, e.getMessage() );
      Assert.assertSame( ioException, e.getCause() );
    }
  }

}
