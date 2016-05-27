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

package org.pentaho.big.data.kettle.plugins.hbase.output;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceLocator;
import org.pentaho.big.data.kettle.plugins.hbase.NamedClusterLoadSaveUtil;
import org.pentaho.bigdata.api.hbase.HBaseService;
import org.pentaho.bigdata.api.hbase.mapping.Mapping;
import org.pentaho.bigdata.api.hbase.mapping.MappingFactory;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

  List<DatabaseMeta> databases = new ArrayList<>();

  @InjectMocks HBaseOutputMeta hBaseOutputMeta;

  @Test
  public void testReadRepSetsNamedCluster() throws Exception {
    when( namedClusterLoadSaveUtil.loadClusterConfig( namedClusterService, id_step, rep, metaStore, null, null ) )
      .thenReturn( namedCluster );
    HBaseService service = mock( HBaseService.class );
    when( namedClusterServiceLocator.getService( namedCluster, HBaseService.class ) ).thenReturn(  service );
    when( service.getMappingFactory() )
      .thenReturn( mock( MappingFactory.class ) );
    Mapping mapping = mock( Mapping.class );
    when( mapping.readRep( rep, id_step ) ).thenReturn( true );
    when( service.getMappingFactory().createMapping() ).thenReturn( mapping );

    hBaseOutputMeta.readRep( rep, metaStore, id_step, databases );
    assertThat( hBaseOutputMeta.getNamedCluster(), is( namedCluster ) );
    assertThat( hBaseOutputMeta.getMapping(), is( mapping ) );
  }
}
