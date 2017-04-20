/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2017-2017 by Pentaho : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.mapreduce.entry.pmr;


import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.KettleClientEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.variables.Variables;
import org.mockito.Mockito;
import org.pentaho.di.job.Job;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;


public class JobEntryHadoopTransJobExecutorTest {

  Repository repository;
  RepositoryDirectoryInterface directoryInterface;

  @Before
  public void setup() throws KettleException {
    KettleClientEnvironment.init();
    repository = Mockito.mock( Repository.class );
    directoryInterface = Mockito.mock( RepositoryDirectoryInterface.class );
  }

  @Test
  public void testProperVariableSpaceWhenLoadTransMetaFromRepo() throws Throwable {
    JobEntryHadoopTransJobExecutor jobEntry = Mockito.spy( new JobEntryHadoopTransJobExecutor( null, null, null, null ) );
    String dir = "repo/path";
    String file = "testName";
    String dirVar = "TestVariablePath";
    String fileVar = "TestVariableName";
    Variables variables = new Variables();
    variables.setVariable( dirVar, dir );
    variables.setVariable( fileVar, file );
    Mockito.when( jobEntry.getParentVariableSpace() ).thenReturn( variables );
    Mockito.when( jobEntry.getParentJob() ).thenReturn( Mockito.mock( Job.class ) );
    Mockito.when( repository.loadRepositoryDirectoryTree() ).thenReturn( directoryInterface );
    Mockito.when( directoryInterface.findDirectory( "/" + dir ) ).thenReturn( directoryInterface );
    JobEntryHadoopTransJobExecutor.loadTransMeta( jobEntry, repository, null, null, "/${" + dirVar + "}", "${" + fileVar + "}" );
    Mockito.verify( repository ).loadTransformation( file, directoryInterface, null, true, null );
  }
}
