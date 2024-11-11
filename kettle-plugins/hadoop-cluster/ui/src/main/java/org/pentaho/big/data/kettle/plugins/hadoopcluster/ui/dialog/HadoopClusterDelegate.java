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


package org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog;

import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.AddDriverDialog;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.NamedClusterDialog;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.util.CustomWizardDialog;
import org.pentaho.di.base.AbstractMeta;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.service.PluginServiceLoader;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.locator.api.MetastoreLocator;
import org.pentaho.runtime.test.RuntimeTester;

import java.util.Collection;
import java.util.Map;
import java.util.function.Supplier;

public class HadoopClusterDelegate {

  private static final Class<?> PKG = HadoopClusterDelegate.class;
  private final Supplier<Spoon> spoonSupplier = Spoon::getInstance;

  private final RuntimeTester runtimeTester;
  private final NamedClusterService namedClusterService;

  private static final LogChannelInterface log =
    KettleLogStore.getLogChannelInterfaceFactory().create( "HadoopClusterDelegate" );

  public HadoopClusterDelegate( NamedClusterService clusterService, RuntimeTester tester ) {
    namedClusterService = clusterService;
    runtimeTester = tester;
  }

  public void openDialog( String dialogState, Map<String, String> urlParams ) {
    try {
      Collection<MetastoreLocator> metastoreLocators = PluginServiceLoader.loadServices( MetastoreLocator.class );
      IMetaStore metastore = metastoreLocators.stream().findFirst().get().getMetastore();
      if ( dialogState.equals( "add-driver" ) ) {
        CustomWizardDialog wizardDialog =
          new CustomWizardDialog( spoonSupplier.get().getShell(),
            new AddDriverDialog( (AbstractMeta) spoonSupplier.get().getActiveMeta(), namedClusterService, metastore ) );
        wizardDialog.open();
      } else {
        CustomWizardDialog wizardDialog = new CustomWizardDialog( spoonSupplier.get().getShell(),
          new NamedClusterDialog( namedClusterService, metastore, (AbstractMeta) spoonSupplier.get().getActiveMeta(),
            runtimeTester, urlParams, dialogState ) );
        wizardDialog.open();
      }
    } catch ( Exception e ) {
      log.logError( e.getMessage() );
    }
  }
}
