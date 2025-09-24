/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
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
import java.util.Collections;


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
      Object selection = selectionTreeExtension.getSelection();
      if ( selectionTreeExtension.getAction().equals( Spoon.EDIT_SELECTION_EXTENSION ) ) {
        if ( selection instanceof NamedCluster ) {
          NamedCluster namedCluster = (NamedCluster) selection;
          String name = URLEncoder.encode( namedCluster.getName(), "UTF-8" );
          hadoopClusterDelegate.openDialog( "new-edit", ImmutableMap.of( "name", name ) );
        }
      } else if ( selectionTreeExtension.getAction().equals( Spoon.CREATE_NEW_SELECTION_EXTENSION ) ) {
        if ( selection.equals( NamedCluster.class ) ) {
          hadoopClusterDelegate.openDialog("new-edit", Collections.emptyMap());
        }
      }

    } catch ( UnsupportedEncodingException e ) {
      logChannel.error( e.getMessage() );
    }
  }
}
