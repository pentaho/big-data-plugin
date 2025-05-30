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


package org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.lifecycle;

import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.tree.ThinHadoopClusterFolderProvider;
import org.pentaho.di.core.annotations.LifecyclePlugin;
import org.pentaho.di.core.lifecycle.LifeEventHandler;
import org.pentaho.di.core.lifecycle.LifecycleException;
import org.pentaho.di.core.lifecycle.LifecycleListener;
import org.pentaho.di.ui.spoon.Spoon;

import java.util.function.Supplier;

@LifecyclePlugin( id = "HadoopClusterLifecycleListener" )
public class HadoopClusterLifecycleListener implements LifecycleListener {
  private Supplier<Spoon> spoonSupplier = Spoon::getInstance;

  @Override
  public void onStart( LifeEventHandler handler ) throws LifecycleException {
    Spoon spoon = spoonSupplier.get();
    if ( spoon != null ) {
      spoon.getTreeManager().addTreeProvider( Spoon.STRING_CONFIGURATIONS, new ThinHadoopClusterFolderProvider( ) );
    }
  }

  @Override
  public void onExit( LifeEventHandler handler ) throws LifecycleException {

  }
}
