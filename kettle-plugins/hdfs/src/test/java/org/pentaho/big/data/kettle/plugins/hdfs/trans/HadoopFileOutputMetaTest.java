/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.hdfs.trans;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 11/23/15.
 */
public class HadoopFileOutputMetaTest {

  // for message resolution
  private static Class<?> MessagePKG = HadoopFileOutputMeta.class;
  private NamedClusterService namedClusterService;
  private RuntimeTestActionService runtimeTestActionService;
  private RuntimeTester runtimeTester;


  @Before
  public void setUp() throws Exception {
    namedClusterService = mock( NamedClusterService.class );
    runtimeTestActionService = mock( RuntimeTestActionService.class );
    runtimeTester = mock( RuntimeTester.class );
  }

  /**
   * Tests HadoopFileOutputMeta methods: 1. isFileAsCommand returns false 2. setFileAsCommand is not supported
   */
  @Test
  public void testFileAsCommandOption() {

    HadoopFileOutputMeta hadoopFileOutputMeta = new HadoopFileOutputMeta( namedClusterService, runtimeTestActionService,
      runtimeTester );

    // we expect isFileAsCommand to be false
    assertFalse( hadoopFileOutputMeta.isFileAsCommand() );

    // we expect setFileAsCommand(true or false) to be unsupported
    try {
      hadoopFileOutputMeta.setFileAsCommand( true );
    } catch ( Exception e ) {
      // the expected message is "class name":" message from the package that HadoopFileOutputMeta is in
      String expectedMessage =
        e.getClass().getName() + ": "
          + BaseMessages.getString( MessagePKG, "HadoopFileOutput.MethodNotSupportedException.Message" );
      assertTrue( e.getMessage().equals( expectedMessage ) );
    }
  }

  @Test
  public void testProcessedUrl() throws Exception {
    String sourceConfigurationName = "scName";
    String desiredUrl = "desiredUrl";
    String url = "url";
    HadoopFileOutputMeta hadoopFileOutputMeta = new HadoopFileOutputMeta( namedClusterService, runtimeTestActionService,
      runtimeTester );
    IMetaStore metaStore = mock( IMetaStore.class );
    assertTrue( null == hadoopFileOutputMeta.getProcessedUrl( metaStore, null ));
    hadoopFileOutputMeta.setSourceConfigurationName( sourceConfigurationName );
    NamedCluster nc = mock( NamedCluster.class );
    when( namedClusterService.getNamedClusterByName( eq( sourceConfigurationName ), (IMetaStore) anyObject() ) ).thenReturn( null );
    assertEquals( url, hadoopFileOutputMeta.getProcessedUrl( metaStore, url ) );
    when( namedClusterService.getNamedClusterByName( eq( sourceConfigurationName ), (IMetaStore) anyObject() ) ).thenReturn( nc );
    when( nc.processURLsubstitution( eq( url ), (IMetaStore) anyObject(), (VariableSpace) anyObject() ) ).thenReturn( desiredUrl );
    assertEquals( desiredUrl, hadoopFileOutputMeta.getProcessedUrl( metaStore, url ) );
  }
}
