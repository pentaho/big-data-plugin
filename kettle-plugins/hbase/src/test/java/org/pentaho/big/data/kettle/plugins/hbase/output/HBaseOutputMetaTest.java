/*******************************************************************************
 *
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.hbase.output;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceLocator;
import org.pentaho.big.data.kettle.plugins.hbase.LogInjector;
import org.pentaho.big.data.kettle.plugins.hbase.MappingDefinition;
import org.pentaho.big.data.kettle.plugins.hbase.NamedClusterLoadSaveUtil;
import org.pentaho.bigdata.api.hbase.HBaseService;
import org.pentaho.bigdata.api.hbase.mapping.Mapping;
import org.pentaho.bigdata.api.hbase.mapping.MappingFactory;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LoggingBuffer;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

@RunWith( org.mockito.runners.MockitoJUnitRunner.class )
public class HBaseOutputMetaTest {

  @Mock NamedClusterService namedClusterService;
  @Mock NamedClusterServiceLocator namedClusterServiceLocator;
  @Mock RuntimeTestActionService runtimeTestActionService;
  @Mock RuntimeTester runtimeTester;
  @Mock NamedClusterLoadSaveUtil namedClusterLoadSaveUtil;
  @Mock NamedCluster namedCluster;

  @Mock Repository rep;
  @Mock IMetaStore metaStore;
  @Mock ObjectId id_step;
  @Mock HBaseService hBaseService;
  @Mock MappingDefinition mappingDefinition;

  List<DatabaseMeta> databases = new ArrayList<>();

  @InjectMocks HBaseOutputMeta hBaseOutputMeta;

  @Test
  public void testReadRepSetsNamedCluster() throws Exception {
    when( namedClusterLoadSaveUtil.loadClusterConfig( any(), any(), any(), any(), any(), any() ) ).thenReturn( namedCluster );
    when( namedClusterServiceLocator.getService( namedCluster, HBaseService.class ) ).thenReturn( hBaseService );
    HBaseOutputMeta hBaseOutputMetaSpy = Mockito.spy( this.hBaseOutputMeta );
    when( hBaseOutputMetaSpy.getHBaseLocalService() ).thenReturn( hBaseService );
    when( hBaseService.getMappingFactory() ).thenReturn( mock( MappingFactory.class ) );
    Mapping mapping = mock( Mapping.class );
    when( mapping.readRep( rep, id_step ) ).thenReturn( true );
    when( hBaseService.getMappingFactory().createMapping() ).thenReturn( mapping );
    hBaseOutputMetaSpy.readRep( rep, metaStore, id_step, databases );
    assertThat( hBaseOutputMetaSpy.getNamedCluster(), is( namedCluster ) );
    assertThat( hBaseOutputMetaSpy.getMapping(), is( mapping ) );
  }

  /**
   * actual for bug BACKLOG-9529
   */
  @Test
  public void testLogSuccessfulForGetXml() throws Exception {
    HBaseOutputMeta hBaseOutputMetaSpy = Mockito.spy( this.hBaseOutputMeta );
    Mockito.doThrow( new KettleException( "Unexpected error occured" ) ).when( hBaseOutputMetaSpy ).applyInjection( any() );

    LoggingBuffer loggingBuffer = LogInjector.setMockForLoggingBuffer();
    hBaseOutputMetaSpy.getXML();
    verify( loggingBuffer, atLeast( 1 ) ).addLogggingEvent( any() );
  }

  /**
   * actual for bug BACKLOG-9629
   */
  @Test
  public void testApplyInjectionDefinitionExists() throws Exception {
    HBaseOutputMeta hBaseOutputMetaSpy = Mockito.spy( this.hBaseOutputMeta );
    when( namedClusterServiceLocator.getService( namedCluster, HBaseService.class ) ).thenReturn( hBaseService );
    hBaseOutputMetaSpy.setMappingDefinition( mappingDefinition );
    hBaseOutputMetaSpy.setNamedCluster (namedCluster);
    Mockito.doReturn( null ).when( hBaseOutputMetaSpy ).getMapping( any(), any() );

    hBaseOutputMetaSpy.getXML( );
    verify( hBaseOutputMetaSpy, times( 1 ) ).setMapping( any() );
  }

  /**
   * actual for bug BACKLOG-9629
   */
  @Test
  public void testApplyInjectionDefinitionNull () throws Exception {
    HBaseOutputMeta hBaseOutputMetaSpy = Mockito.spy( this.hBaseOutputMeta );
    when( namedClusterServiceLocator.getService( namedCluster, HBaseService.class ) ).thenReturn( hBaseService );
    hBaseOutputMetaSpy.setMappingDefinition( null );
    hBaseOutputMetaSpy.setNamedCluster ( namedCluster );
    Mockito.doReturn( null ).when( hBaseOutputMetaSpy ).getMapping( any(), any() );

    hBaseOutputMetaSpy.getXML();
    verify( hBaseOutputMetaSpy, times( 0 ) ).getMapping( any(), any() );
    verify( hBaseOutputMetaSpy, times( 0 ) ).setMapping( any() );
  }
}
