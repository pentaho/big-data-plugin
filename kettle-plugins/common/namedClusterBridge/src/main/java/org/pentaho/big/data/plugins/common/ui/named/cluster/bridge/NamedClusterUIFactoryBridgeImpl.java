/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package org.pentaho.big.data.plugins.common.ui.named.cluster.bridge;

import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.big.data.kettle.plugins.hdfs.vfs.HadoopVfsFileChooserDialog;
import org.pentaho.big.data.plugins.common.ui.HadoopClusterDelegateImpl;
import org.pentaho.big.data.plugins.common.ui.NamedClusterDialogImpl;
import org.pentaho.big.data.plugins.common.ui.NamedClusterWidgetImpl;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.ui.core.namedcluster.HadoopClusterDelegate;
import org.pentaho.di.ui.core.namedcluster.NamedClusterDialog;
import org.pentaho.di.ui.core.namedcluster.NamedClusterUIFactory;
import org.pentaho.di.ui.core.namedcluster.NamedClusterUIHelper;
import org.pentaho.di.ui.core.namedcluster.NamedClusterWidget;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;
import org.pentaho.vfs.ui.CustomVfsUiPanel;
import org.pentaho.vfs.ui.VfsFileChooserDialog;

/**
 * Created by bryan on 8/17/15.
 *
 * @deprecated
 */
@Deprecated
public class NamedClusterUIFactoryBridgeImpl implements NamedClusterUIFactory {
  private final NamedClusterService namedClusterService;
  private final RuntimeTestActionService runtimeTestActionService;
  private final RuntimeTester runtimeTester;

  public NamedClusterUIFactoryBridgeImpl( NamedClusterService namedClusterService,
                                          RuntimeTestActionService runtimeTestActionService,
                                          RuntimeTester runtimeTester ) {
    this.namedClusterService = namedClusterService;
    this.runtimeTestActionService = runtimeTestActionService;
    this.runtimeTester = runtimeTester;
    NamedClusterUIHelper.setNamedClusterUIFactory( this );
  }

  @Override public NamedClusterWidget createNamedClusterWidget( Composite parent, boolean showLabel ) {
    return new NamedClusterWidgetBridgedImpl(
      new NamedClusterWidgetImpl( parent, showLabel, namedClusterService, runtimeTestActionService, runtimeTester ) );
  }

  @Override public HadoopClusterDelegate createHadoopClusterDelegate( Spoon spoon ) {
    return new HadoopClusterDelegateBridgeImpl(
      new HadoopClusterDelegateImpl( spoon, namedClusterService, runtimeTestActionService, runtimeTester ) );
  }

  @Override public NamedClusterDialog createNamedClusterDialog( Shell shell ) {
    return new NamedClusterDialogBridgeImpl( new NamedClusterDialogImpl( shell, namedClusterService,
      runtimeTestActionService, runtimeTester ) );
  }

  @Override public NamedCluster getNamedClusterFromVfsFileChooser( Spoon spoon ) {
    VfsFileChooserDialog dialog = spoon.getVfsFileChooserDialog( null, null );
    CustomVfsUiPanel currentPanel = dialog.getCurrentPanel();
    if ( currentPanel != null && currentPanel instanceof HadoopVfsFileChooserDialog ) {
      HadoopVfsFileChooserDialog hadoopVfsFileChooserDialog = (HadoopVfsFileChooserDialog) currentPanel;
      NamedClusterWidgetImpl ncWidget = hadoopVfsFileChooserDialog.getNamedClusterWidget();
      return NamedClusterBridgeImpl.fromOsgiNamedCluster( ncWidget.getSelectedNamedCluster() );
    }
    return null;
  }
}
