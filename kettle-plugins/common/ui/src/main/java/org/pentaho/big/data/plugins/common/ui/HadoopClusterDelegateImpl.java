/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.delegates.SpoonDelegate;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;

public class HadoopClusterDelegateImpl extends SpoonDelegate {
  public static final String SPOON_DIALOG_ERROR_DELETING_NAMED_CLUSTER_TITLE =
    "Spoon.Dialog.ErrorDeletingNamedCluster.Title";
  public static final String SPOON_DIALOG_ERROR_DELETING_NAMED_CLUSTER_MESSAGE =
    "Spoon.Dialog.ErrorDeletingNamedCluster.Message";
  public static final String SPOON_VARIOUS_DUPE_NAME = "Spoon.Various.DupeName";
  public static final String SPOON_DIALOG_ERROR_SAVING_NAMED_CLUSTER_TITLE =
    "Spoon.Dialog.ErrorSavingNamedCluster.Title";
  public static final String SPOON_DIALOG_ERROR_SAVING_NAMED_CLUSTER_MESSAGE =
    "Spoon.Dialog.ErrorSavingNamedCluster.Message";
  public static Class<?> PKG = HadoopClusterDelegateImpl.class; // for i18n purposes, needed by Translator2!!

  private final NamedClusterService namedClusterService;
  private final RuntimeTestActionService runtimeTestActionService;
  private final RuntimeTester runtimeTester;
  private final CommonDialogFactory commonDialogFactory;

  public HadoopClusterDelegateImpl( Spoon spoon, NamedClusterService namedClusterService,
                                    RuntimeTestActionService runtimeTestActionService, RuntimeTester runtimeTester ) {
    this( spoon, namedClusterService, runtimeTestActionService, runtimeTester, new CommonDialogFactory() );
  }

  public HadoopClusterDelegateImpl( Spoon spoon, NamedClusterService namedClusterService,
                                    RuntimeTestActionService runtimeTestActionService, RuntimeTester runtimeTester,
                                    CommonDialogFactory commonDialogFactory ) {
    super( spoon );
    this.namedClusterService = namedClusterService;
    this.runtimeTestActionService = runtimeTestActionService;
    this.runtimeTester = runtimeTester;
    this.commonDialogFactory = commonDialogFactory;
  }

  public void dupeNamedCluster( IMetaStore metaStore, NamedCluster nc, Shell shell ) {
    if ( metaStore == null ) {
      metaStore = spoon.getMetaStore();
    }
    if ( nc != null ) {
      NamedCluster ncCopy = nc.clone();
      // The "duplicate name" string comes from Spoon, so use its class to get the resource
      String dupename = BaseMessages.getString( Spoon.class, SPOON_VARIOUS_DUPE_NAME ) + nc.getName();
      ncCopy.setName( dupename );

      NamedClusterDialogImpl namedClusterDialogImpl = commonDialogFactory
        .createNamedClusterDialog( shell, namedClusterService, runtimeTestActionService, runtimeTester, ncCopy );
      namedClusterDialogImpl.setNewClusterCheck( true );
      String newname = namedClusterDialogImpl.open();

      if ( newname != null ) { // null: CANCEL
        saveNamedCluster( metaStore, ncCopy );
        spoon.refreshTree();
      }
    }
  }

  public void delNamedCluster( IMetaStore metaStore, NamedCluster namedCluster ) {
    if ( metaStore == null ) {
      metaStore = spoon.getMetaStore();
    }
    deleteNamedCluster( metaStore, namedCluster );
    spoon.refreshTree();
    spoon.setShellText();
  }

  public String editNamedCluster( IMetaStore metaStore, NamedCluster namedCluster, Shell shell ) {
    if ( metaStore == null ) {
      metaStore = spoon.getMetaStore();
    }
    NamedClusterDialogImpl namedClusterDialogImpl = commonDialogFactory.createNamedClusterDialog( shell,
      namedClusterService, runtimeTestActionService, runtimeTester, namedCluster.clone() );
    namedClusterDialogImpl.setNewClusterCheck( false );
    String result = namedClusterDialogImpl.open();
    if ( result != null ) {
      deleteNamedCluster( metaStore, namedCluster );
      saveNamedCluster( metaStore, namedClusterDialogImpl.getNamedCluster() );
      spoon.refreshTree();
      if ( namedClusterDialogImpl.getNamedCluster() != null ) {
        return namedClusterDialogImpl.getNamedCluster().getName();
      }
    }
    return null;
  }

  public String newNamedCluster( VariableSpace variableSpace, IMetaStore metaStore, Shell shell ) {
    if ( metaStore == null ) {
      metaStore = spoon.getMetaStore();
    }

    NamedCluster nc = namedClusterService.getClusterTemplate();

    NamedClusterDialogImpl namedClusterDialogImpl = commonDialogFactory
      .createNamedClusterDialog( shell, namedClusterService, runtimeTestActionService, runtimeTester, nc );
    namedClusterDialogImpl.setNewClusterCheck( true );
    String result = namedClusterDialogImpl.open();

    if ( result != null ) {
      if ( variableSpace != null ) {
        nc.shareVariablesWith( (VariableSpace) variableSpace );
      } else {
        nc.initializeVariablesFrom( null );
      }

      saveNamedCluster( metaStore, nc );
      spoon.refreshTree();
      return nc.getName();
    }
    return null;
  }

  private void deleteNamedCluster( IMetaStore metaStore, NamedCluster namedCluster ) {
    try {
      if ( namedClusterService.read( namedCluster.getName(), metaStore ) != null ) {
        namedClusterService.delete( namedCluster.getName(), metaStore );
      }
    } catch ( MetaStoreException e ) {
      commonDialogFactory.createErrorDialog( spoon.getShell(),
        BaseMessages.getString( PKG, SPOON_DIALOG_ERROR_DELETING_NAMED_CLUSTER_TITLE ),
        BaseMessages.getString( PKG, SPOON_DIALOG_ERROR_DELETING_NAMED_CLUSTER_MESSAGE, namedCluster.getName() ), e );
    }
  }

  private void saveNamedCluster( IMetaStore metaStore, NamedCluster namedCluster ) {
    try {
      namedClusterService.create( namedCluster, metaStore );
    } catch ( MetaStoreException e ) {
      commonDialogFactory.createErrorDialog( spoon.getShell(),
        BaseMessages.getString( PKG, SPOON_DIALOG_ERROR_SAVING_NAMED_CLUSTER_TITLE ),
        BaseMessages.getString( PKG, SPOON_DIALOG_ERROR_SAVING_NAMED_CLUSTER_MESSAGE, namedCluster.getName() ), e );
    }
  }

}
