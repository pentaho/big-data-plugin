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

package org.pentaho.big.data.plugins.common.ui.named.cluster.bridge;

import org.eclipse.swt.widgets.Shell;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.pentaho.big.data.plugins.common.ui.HadoopClusterDelegateImpl;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.metastore.api.IMetaStore;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Created by bryan on 8/28/15.
 */
public class HadoopClusterDelegateBridgeImplTest {
  private HadoopClusterDelegateImpl hadoopClusterDelegate;
  private HadoopClusterDelegateBridgeImpl hadoopClusterDelegateBridge;
  private IMetaStore iMetaStore;
  private VariableSpace variableSpace;
  private NamedCluster namedCluster;
  private Shell shell;
  private String testNamedClusterName;

  private static org.pentaho.hadoop.shim.api.cluster.NamedCluster nameMatches( NamedCluster namedCluster ) {
    return argThat( new NamedClusterNameMatcher( namedCluster ) );
  }

  @Before
  public void setup() {
    hadoopClusterDelegate = mock( HadoopClusterDelegateImpl.class );
    hadoopClusterDelegateBridge = new HadoopClusterDelegateBridgeImpl( hadoopClusterDelegate );
    iMetaStore = mock( IMetaStore.class );
    variableSpace = mock( VariableSpace.class );
    namedCluster = mock( NamedCluster.class );
    testNamedClusterName = "testNamedClusterName";
    when( namedCluster.getName() ).thenReturn( testNamedClusterName );
    shell = mock( Shell.class );
  }

  @Test
  public void testEditNamedCluster() {
    String edit = "edit";
    when( hadoopClusterDelegate.editNamedCluster( eq( iMetaStore ), nameMatches( namedCluster ), eq( shell ) ) )
      .thenReturn( edit );
    assertEquals( edit, hadoopClusterDelegateBridge.editNamedCluster( iMetaStore, namedCluster, shell ) );
  }

  @Test
  public void testNewNamedCluster() {
    String newName = "new";
    when( hadoopClusterDelegate.newNamedCluster( variableSpace, iMetaStore, shell ) ).thenReturn( newName );
    assertEquals( newName, hadoopClusterDelegateBridge.newNamedCluster( variableSpace, iMetaStore, shell ) );
  }

  @Test
  public void testDupeNamedCluster() {
    hadoopClusterDelegateBridge.dupeNamedCluster( iMetaStore, namedCluster, shell );
    verify( hadoopClusterDelegate ).dupeNamedCluster( eq( iMetaStore ), nameMatches( namedCluster ), eq( shell ) );
  }

  @Test
  public void testDelNamedCluster() {
    hadoopClusterDelegateBridge.delNamedCluster( iMetaStore, namedCluster );
    verify( hadoopClusterDelegate ).delNamedCluster( eq( iMetaStore ), nameMatches( namedCluster ) );
  }

  private static class NamedClusterNameMatcher extends ArgumentMatcher<org.pentaho.hadoop.shim.api.cluster.NamedCluster> {
    private final NamedCluster namedCluster;

    NamedClusterNameMatcher( NamedCluster namedCluster ) {
      this.namedCluster = namedCluster;
    }

    @Override public boolean matches( Object argument ) {
      return argument instanceof org.pentaho.hadoop.shim.api.cluster.NamedCluster && ( (org.pentaho.hadoop.shim.api.cluster.NamedCluster) argument ).getName().equals( namedCluster.getName() );
    }
  }
}
