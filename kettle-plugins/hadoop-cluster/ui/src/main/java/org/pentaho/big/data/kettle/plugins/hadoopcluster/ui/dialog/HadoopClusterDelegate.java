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


package org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog;

import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.spoon.Spoon;

import java.util.Map;
import java.util.function.Supplier;

public class HadoopClusterDelegate {

  private static final Class<?> PKG = HadoopClusterDelegate.class;
  private Supplier<Spoon> spoonSupplier = Spoon::getInstance;

  private static final int WIDTH = 630;
  private static final int HEIGHT = 650;

  public void openDialog( String thinAppState, Map<String, String> urlParams ) {
    HadoopClusterDialog hadoopClusterDialog = new HadoopClusterDialog( spoonSupplier.get().getShell(), WIDTH, HEIGHT );
    hadoopClusterDialog.open(
      BaseMessages.getString( PKG, "HadoopCluster.dialog.title" ), thinAppState, urlParams );
  }
}
