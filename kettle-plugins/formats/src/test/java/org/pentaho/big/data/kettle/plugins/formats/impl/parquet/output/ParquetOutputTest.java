/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2019 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.big.data.kettle.plugins.formats.impl.parquet.output;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.named.cluster.NamedClusterEmbedManager;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class ParquetOutputTest {

  @Mock StepMeta stepMeta;
  @Mock StepDataInterface stepDataInterface;
  @Mock TransMeta transMeta;
  @Mock Trans trans;
  @Mock NamedClusterServiceLocator namedClusterServiceLocator;
  private ParquetOutput parquetOutput;

  @Before
  public void setUp() throws Exception {
    when( transMeta.getName() ).thenReturn( "transName" );
    when( stepMeta.getName() ).thenReturn( "stepName" );
    when( transMeta.findStep( "stepName" ) ).thenReturn( stepMeta );
    parquetOutput = new ParquetOutput( stepMeta, stepDataInterface, 1, transMeta,
      trans, namedClusterServiceLocator );
  }

  @Test
  public void initShouldPassEmbeddedMetastoreKey() {
    ParquetOutputMeta stepMetaInterface = mock( ParquetOutputMeta.class );
    ParquetOutputData stepDataInterface = mock( ParquetOutputData.class );
    NamedClusterEmbedManager namedClusterEmbedManager = mock( NamedClusterEmbedManager.class );
    when( transMeta.getNamedClusterEmbedManager() ).thenReturn( namedClusterEmbedManager );
    when( transMeta.getEmbeddedMetastoreProviderKey() ).thenReturn( "metastoreProviderKey" );
    parquetOutput.init( stepMetaInterface, stepDataInterface );

    verify( namedClusterEmbedManager ).passEmbeddedMetastoreKey( transMeta, "metastoreProviderKey" );
  }
}
