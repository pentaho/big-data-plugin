/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2024 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.tree;

import com.google.common.collect.ImmutableMap;
import org.pentaho.big.data.impl.cluster.NamedClusterManager;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.HadoopClusterDelegate;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.ui.spoon.SelectionTreeExtension;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.runtime.test.impl.RuntimeTesterImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

@ExtensionPoint( id = "ThinHadoopClusterEditExtension", description = "Edits named cluster",
  extensionPointId = "SpoonViewTreeExtension" )
public class ThinHadoopClusterEditExtension implements ExtensionPointInterface {

  HadoopClusterDelegate hadoopClusterDelegate;
  private static final Logger logChannel = LoggerFactory.getLogger( ThinHadoopClusterEditExtension.class );

  public ThinHadoopClusterEditExtension() {
    this.hadoopClusterDelegate = new HadoopClusterDelegate( NamedClusterManager.getInstance(), RuntimeTesterImpl.getInstance() );
  }

  public ThinHadoopClusterEditExtension( HadoopClusterDelegate hadoopClusterDelegate ) {
    this.hadoopClusterDelegate = hadoopClusterDelegate;
  }

  public void callExtensionPoint( LogChannelInterface log, Object extension ) throws KettleException {
    try {
      SelectionTreeExtension selectionTreeExtension = (SelectionTreeExtension) extension;
      if ( selectionTreeExtension.getAction().equals( Spoon.EDIT_SELECTION_EXTENSION ) ) {
        Object selection = selectionTreeExtension.getSelection();
        if ( selection instanceof NamedCluster ) {
          NamedCluster namedCluster = (NamedCluster) selection;
          String name = URLEncoder.encode( namedCluster.getName(), "UTF-8" );
          hadoopClusterDelegate.openDialog( "new-edit", ImmutableMap.of( "name", name ) );
        }
      }
    } catch ( UnsupportedEncodingException e ) {
      logChannel.error( e.getMessage() );
    }
  }
}
