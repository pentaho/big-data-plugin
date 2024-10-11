/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2017-2017 by Hitachi Vantara : http://www.pentaho.com
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


import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.KettleClientEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.job.Job;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.trans.TransMeta;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.spy;

public class JobEntryHadoopTransJobExecutorTest {

  VariableSpace space;
  Repository repository;
  ObjectId objectId;
  RepositoryDirectoryInterface directoryInterface;

  @Before
  public void setup() throws KettleException {
    KettleClientEnvironment.init();
    space = mock( VariableSpace.class );
    repository = mock( Repository.class );
    objectId = mock( ObjectId.class );
    directoryInterface = mock( RepositoryDirectoryInterface.class );
  }

  @Test
  public void testLoadTransMetaLocal() throws Exception {
    String testPath = "src/test/resources/testTrans.ktr";
    when( space.environmentSubstitute( testPath ) ).thenReturn( testPath );
    TransMeta transMeta = JobEntryHadoopTransJobExecutor.loadTransMeta( space, null, testPath, objectId, null, null );
    Assert.assertEquals( testPath, transMeta.getFilename() );
  }

  @Test
  public void testLoadTransMetaRepo() throws Exception {
    String dir = "/repo/path";
    String file = "testTrans";
    when( space.environmentSubstitute( dir ) ).thenReturn( dir );
    when( space.environmentSubstitute( file ) ).thenReturn( file );
    when( repository.loadRepositoryDirectoryTree() ).thenReturn( directoryInterface );
    when( directoryInterface.findDirectory( dir ) ).thenReturn( directoryInterface );
    JobEntryHadoopTransJobExecutor.loadTransMeta( space, repository, dir + "/" + file, null, null, null );
    verify( repository ).loadTransformation( file, directoryInterface, null, true, null );
  }

  //for backward compatibility
  @Test
  public void testLoadTransMetaRepoReference() throws Exception {
    String dir = "/repo/path";
    String file = "testTrans";
    when( space.environmentSubstitute( dir ) ).thenReturn( dir );
    when( space.environmentSubstitute( file ) ).thenReturn( file );
    when( repository.loadRepositoryDirectoryTree() ).thenReturn( directoryInterface );
    when( directoryInterface.findDirectory( dir ) ).thenReturn( directoryInterface );
    JobEntryHadoopTransJobExecutor.loadTransMeta( space, repository, dir + "/" + file, null, null, null );
    verify( repository ).loadTransformation( file, directoryInterface, null, true, null );
  }

  // for backward compatibility
  @Test
  public void testLoadTransMetaRepoDirFile() throws Exception {
    String dir = "/repo/path";
    String file = "testTrans";
    when( space.environmentSubstitute( dir ) ).thenReturn( dir );
    when( space.environmentSubstitute( file ) ).thenReturn( file );
    when( repository.loadRepositoryDirectoryTree() ).thenReturn( directoryInterface );
    when( directoryInterface.findDirectory( dir ) ).thenReturn( directoryInterface );
    JobEntryHadoopTransJobExecutor.loadTransMeta( space, repository, null, null, dir, file );
    verify( repository ).loadTransformation( file, directoryInterface, null, true, null );
  }

  @Test
  public void testProperVariableSpaceWhenLoadTransMetaFromRepo() throws Throwable {
    JobEntryHadoopTransJobExecutor jobEntry = spy( new JobEntryHadoopTransJobExecutor( null, null, null, null ) );
    String dir = "repo/path";
    String file = "testName";
    String dirVar = "TestVariablePath";
    String fileVar = "TestVariableName";
    jobEntry.setVariable( dirVar, dir );
    jobEntry.setVariable( fileVar, file );
    when( jobEntry.getParentJob() ).thenReturn( mock( Job.class ) );
    when( repository.loadRepositoryDirectoryTree() ).thenReturn( directoryInterface );
    when( directoryInterface.findDirectory( "/" + dir ) ).thenReturn( directoryInterface );
    JobEntryHadoopTransJobExecutor.loadTransMeta( jobEntry, repository, null, null, "/${" + dirVar + "}", "${" + fileVar + "}" );
    verify( repository ).loadTransformation( file, directoryInterface, null, true, null );
  }
}
