/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
 * <p>
 * ******************************************************************************
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 ******************************************************************************/
package org.pentaho.big.data.kettle.plugins.hbase.input;

import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceLocator;
import org.pentaho.big.data.kettle.plugins.hbase.LogInjector;
import org.pentaho.big.data.kettle.plugins.hbase.MappingDefinition;
import org.pentaho.bigdata.api.hbase.HBaseService;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LoggingBuffer;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

@RunWith( org.mockito.runners.MockitoJUnitRunner.class )
public class HBaseInputMetaTest {

  @InjectMocks HBaseInputMeta hBaseInputMeta;
  @Mock NamedCluster namedCluster;
  @Mock NamedClusterServiceLocator namedClusterServiceLocator;
  @Mock HBaseService hBaseService;
  @Mock MappingDefinition mappingDefinition;

  /**
   * actual for bug BACKLOG-9529
   */
  @Test
  public void testLogSuccessfulForGetXml() throws Exception {
    HBaseInputMeta spy = Mockito.spy( hBaseInputMeta );
    spy.setNamedCluster( namedCluster );

    LoggingBuffer loggingBuffer = LogInjector.setMockForLoggingBuffer();

    Mockito.doThrow( new KettleException( "Unexpected error occured" ) ).when( spy ).applyInjection( any() );
    spy.getXML();
    verify( loggingBuffer, atLeast( 1 ) ).addLogggingEvent( any() );
  }

  /**
   * actual for bug BACKLOG-9629
   */
  @SuppressWarnings( "unchecked" )
  @Test
  public void testApplyInjectionDefinitionsExists () throws Exception {
    HBaseInputMeta hBaseInputMetaSpy = Mockito.spy( hBaseInputMeta );
    hBaseInputMetaSpy.setNamedCluster( namedCluster );
    when( namedClusterServiceLocator.getService( namedCluster, HBaseService.class ) ).thenReturn( hBaseService );
    hBaseInputMetaSpy.setMappingDefinition( mappingDefinition );
    List list = mock( List.class );
    hBaseInputMetaSpy.setOutputFieldsDefinition( list );
    hBaseInputMetaSpy.setFiltersDefinition( list );
    Mockito.doReturn( list ).when( hBaseInputMetaSpy ).createOutputFieldsDefinition( any(), any() );
    Mockito.doReturn( list ).when( hBaseInputMetaSpy ).createColumnFiltersFromDefinition( any() );
    Mockito.doReturn( null ).when( hBaseInputMetaSpy ).getMapping( any(), any() );

    hBaseInputMetaSpy.getXML( );
    verify( hBaseInputMetaSpy, times( 1 ) ).setMapping ( any() );
    verify( hBaseInputMetaSpy, times( 1 ) ).setOutputFields ( any() );
    verify( hBaseInputMetaSpy, times( 1 ) ).setColumnFilters ( any() );
  }

  /**
   * actual for bug BACKLOG-9629
   */
  @Test
  public void testApplyInjectionDefinitionsNull () throws Exception {
    HBaseInputMeta hBaseInputMetaSpy = Mockito.spy( hBaseInputMeta );
    hBaseInputMetaSpy.setNamedCluster( namedCluster );
    when( namedClusterServiceLocator.getService( namedCluster, HBaseService.class ) ).thenReturn( hBaseService );
    hBaseInputMetaSpy.setMappingDefinition( null );
    hBaseInputMetaSpy.setOutputFieldsDefinition( null );
    hBaseInputMetaSpy.setFiltersDefinition( null );

    hBaseInputMetaSpy.getXML();
    verify( hBaseInputMetaSpy, times( 0 ) ).setMapping ( any() );
    verify( hBaseInputMetaSpy, times( 0 ) ).getMapping ();
    verify( hBaseInputMetaSpy, times( 0 ) ).setOutputFields ( any() );
    verify( hBaseInputMetaSpy, times( 0 ) ).setColumnFilters ( any() );
  }
}
