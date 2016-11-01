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
package org.pentaho.big.data.kettle.plugins.mapreduce.ui.entry.pmr;

import org.mockito.Matchers;
import org.mockito.Mockito;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.big.data.plugins.common.ui.HadoopClusterDelegateImpl;

public class JobEntryHadoopTransJobExecutorControllerTest {
  private JobEntryHadoopTransJobExecutorController testController;
  private HadoopClusterDelegateImpl ncDelegateMock = Mockito.mock( HadoopClusterDelegateImpl.class );
  private NamedClusterService namedClusterServiceMock = Mockito.mock( NamedClusterService.class );


  @Before
  public void setUp() throws Throwable {
    testController = new JobEntryHadoopTransJobExecutorController( ncDelegateMock, namedClusterServiceMock );
    // do this controller as spy to check
    testController = Mockito.spy( testController );
  }


  @Test
  public void testExtractDirFileFromRepositoryByName() throws Exception {
    String validPath = "/path/to/file";
    String invalidPathWithoutName = "/path/to/";
    String invalid_test_1 = "smth/ ";
    String invalid_test_2 = "invalidPath";
    String invalid_test_3 = "   1 ";
    testController.extractDirFileRepositoryTask( invalidPathWithoutName, JobEntryHadoopTransJobExecutorController.MAPPER );
    testController.extractDirFileRepositoryTask( invalid_test_1, JobEntryHadoopTransJobExecutorController.REDUCER );
    testController.extractDirFileRepositoryTask( invalid_test_2, JobEntryHadoopTransJobExecutorController.COMBINER );
    testController.extractDirFileRepositoryTask( invalid_test_3, JobEntryHadoopTransJobExecutorController.MAPPER );
    //ensure that the invalid paths were not set
    Mockito.verify( testController, Mockito.never() ).setMapRepositoryDir( Matchers.anyString() );
    Mockito.verify( testController, Mockito.never() ).setMapRepositoryFile( Matchers.anyString() );
    Mockito.verify( testController, Mockito.never() ).setReduceRepositoryDir( Matchers.anyString() );
    Mockito.verify( testController, Mockito.never() ).setReduceRepositoryFile( Matchers.anyString() );
    Mockito.verify( testController, Mockito.never() ).setCombinerRepositoryDir( Matchers.anyString() );
    Mockito.verify( testController, Mockito.never() ).setCombinerRepositoryFile( Matchers.anyString() );
    testController.extractDirFileRepositoryTask( validPath, JobEntryHadoopTransJobExecutorController.COMBINER );
    //ensure that the valid path was parsed successfully
    Mockito.verify( testController, Mockito.times( 1 ) ).setCombinerRepositoryDir( "/path/to" );
    Mockito.verify( testController, Mockito.times( 1 ) ).setCombinerRepositoryFile( "file" );

  }
}
