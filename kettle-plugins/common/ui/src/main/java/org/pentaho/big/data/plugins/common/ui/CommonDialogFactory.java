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
