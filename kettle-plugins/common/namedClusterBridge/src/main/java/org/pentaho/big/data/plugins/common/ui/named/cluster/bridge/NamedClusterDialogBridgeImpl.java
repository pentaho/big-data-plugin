/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

import org.pentaho.big.data.plugins.common.ui.NamedClusterDialogImpl;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.ui.core.namedcluster.NamedClusterDialog;

/**
 * Created by bryan on 8/17/15.
 *
 * @deprecated
 */
@Deprecated
public class NamedClusterDialogBridgeImpl implements NamedClusterDialog {
  private final NamedClusterDialogImpl delegate;

  public NamedClusterDialogBridgeImpl( NamedClusterDialogImpl delegate ) {
    this.delegate = delegate;
  }

  @Override public NamedCluster getNamedCluster() {
    return NamedClusterBridgeImpl.fromOsgiNamedCluster( delegate.getNamedCluster() );
  }

  @Override public void setNamedCluster( NamedCluster namedCluster ) {
    delegate.setNamedCluster( new NamedClusterBridgeImpl( namedCluster ) );
  }

  @Override public void setNewClusterCheck( boolean newClusterCheck ) {
    delegate.setNewClusterCheck( newClusterCheck );
  }

  @Override public String open() {
    return delegate.open();
  }
}
