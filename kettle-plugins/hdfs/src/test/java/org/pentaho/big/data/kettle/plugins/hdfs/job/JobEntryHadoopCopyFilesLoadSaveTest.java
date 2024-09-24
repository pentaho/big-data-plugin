/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.hdfs.job;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.hadoop.shim.api.cluster.ClusterInitializationException;
import org.pentaho.big.data.impl.cluster.NamedClusterImpl;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.job.entry.loadSave.LoadSaveTester;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;

public class JobEntryHadoopCopyFilesLoadSaveTest {

  private NamedClusterService namedClusterService;
  private RuntimeTestActionService runtimeTestActionService;
  private RuntimeTester runtimeTester;

  @Before
  public void setup() throws ClusterInitializationException {
    namedClusterService = mock( NamedClusterService.class );
    when( namedClusterService.getClusterTemplate() ).thenReturn( new NamedClusterImpl() );
    mock( NamedClusterServiceLocator.class );
    runtimeTester = mock( RuntimeTester.class );
    runtimeTestActionService = mock( RuntimeTestActionService.class );
    new JobEntryHadoopCopyFiles( namedClusterService, runtimeTestActionService, runtimeTester );
  }

  @Test
  public void testLoadSave() throws KettleException {
    List<String> commonAttributes = Arrays.asList( "copy_empty_folders", "arg_from_previous", "overwrite_files",
      "include_subfolders", "remove_source_files", "add_result_filesname", "destination_is_a_file",
      "create_destination_folder" );

    Map<String, String> getterMap = new HashMap<String, String>();
    getterMap.put( "copy_empty_folders", "isCopyEmptyFolders" );
    getterMap.put( "arg_from_previous", "isArgFromPrevious" );
    getterMap.put( "overwrite_files", "isoverwrite_files" );
    getterMap.put( "include_subfolders", "isIncludeSubfolders" );
    getterMap.put( "remove_source_files", "isRemoveSourceFiles" );
    getterMap.put( "add_result_filesname", "isAddresultfilesname" );
    getterMap.put( "destination_is_a_file", "isDestinationIsAFile" );
    getterMap.put( "create_destination_folder", "isCreateDestinationFolder" );

    Map<String, String> setterMap = new HashMap<String, String>();
    setterMap.put( "copy_empty_folders", "setCopyEmptyFolders" );
    setterMap.put( "arg_from_previous", "setArgFromPrevious" );
    setterMap.put( "overwrite_files", "setoverwrite_files" );
    setterMap.put( "include_subfolders", "setIncludeSubfolders" );
    setterMap.put( "remove_source_files", "setRemoveSourceFiles" );
    setterMap.put( "add_result_filesname", "setAddresultfilesname" );
    setterMap.put( "destination_is_a_file", "setDestinationIsAFile" );
    setterMap.put( "create_destination_folder", "setCreateDestinationFolder" );

    LoadSaveTester<JobEntryHadoopCopyFiles> tester =
      new LoadSaveTester<JobEntryHadoopCopyFiles>( JobEntryHadoopCopyFiles.class, commonAttributes,
        getterMap, setterMap ) {
        @Override public JobEntryHadoopCopyFiles createMeta() {
          return new JobEntryHadoopCopyFiles( namedClusterService, runtimeTestActionService, runtimeTester );
        }
      };

    tester.testSerialization();
  }
}
