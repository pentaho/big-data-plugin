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


package org.pentaho.big.data.plugins.common.ui;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;

/**
 * Created by bryan on 10/19/15.
 */
public class CommonDialogFactory {
  public void createErrorDialog( Shell parent, String title, String message, Exception exception ) {
    new ErrorDialog( parent, title, message, exception );
  }

  public NamedClusterDialogImpl createNamedClusterDialog( Shell parent, NamedClusterService namedClusterService,
                                                          RuntimeTestActionService runtimeTestActionService,
                                                          RuntimeTester runtimeTester,
                                                          NamedCluster namedCluster ) {
    return new NamedClusterDialogImpl( parent, namedClusterService, runtimeTestActionService, runtimeTester,
      namedCluster );
  }
}
