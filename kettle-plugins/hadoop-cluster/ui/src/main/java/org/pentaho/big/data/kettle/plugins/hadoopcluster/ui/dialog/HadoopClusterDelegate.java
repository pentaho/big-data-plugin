/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog;

import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.spoon.Spoon;

import java.util.function.Supplier;

public class HadoopClusterDelegate {

  private static final Class<?> PKG = HadoopClusterDelegate.class;
  private Supplier<Spoon> spoonSupplier = Spoon::getInstance;

  private static final int WIDTH = 630;
  private static final int HEIGHT = 650;

  public void openDialog() {
    HadoopClusterDialog
      hadoopClusterDialog = new HadoopClusterDialog( spoonSupplier.get().getShell(), WIDTH, HEIGHT );
    hadoopClusterDialog.open( BaseMessages.getString( PKG, "HadoopCluster.dialog.new.title" ) );
  }

  public void openDialog( String label ) {
    HadoopClusterDialog
      hadoopClusterDialog = new HadoopClusterDialog( spoonSupplier.get().getShell(), WIDTH, HEIGHT );
    hadoopClusterDialog.open( BaseMessages.getString( PKG, "HadoopCluster.dialog.edit.title" ), label );
  }

}
