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

import org.apache.commons.io.FileUtils;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.delegates.SpoonDelegate;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.stores.delegate.DelegatingMetaStore;
import org.pentaho.metastore.stores.xml.XmlMetaStore;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

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
  public static final String STRING_NAMED_CLUSTERS = BaseMessages.getString( PKG, "NamedClusterDialog.HadoopClusters" );

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

    if ( nc == null ) {
      return;
    }

    NamedCluster newNamedCluster = nc.clone();

    // The "duplicate name" string comes from Spoon, so use its class to get the resource
    String duplicateName = BaseMessages.getString( Spoon.class, SPOON_VARIOUS_DUPE_NAME ) + nc.getName();
    newNamedCluster.setName( duplicateName );

    NamedClusterDialogImpl namedClusterDialogImpl = commonDialogFactory
      .createNamedClusterDialog( shell, namedClusterService, runtimeTestActionService, runtimeTester, newNamedCluster );
    namedClusterDialogImpl.setNewClusterCheck( true );

    String newClusterName = namedClusterDialogImpl.open();
    // Check if the process was cancelled
    if ( newClusterName == null ) {
      return;
    }

    try {
      XmlMetaStore xmlMetaStore = getXmlMetastore( metaStore );

      if ( xmlMetaStore != null ) {
        if ( newNamedCluster.getName() != null ) {
          delNamedCluster( metaStore, newNamedCluster );
        }

        File sourceClusterConfigDir = new File( getNamedClusterConfigsRootDir( xmlMetaStore ) + "/" + nc.getName() );
        File newClusterConfigDir = new File( getNamedClusterConfigsRootDir( xmlMetaStore ) + "/" + newClusterName );
        saveNamedCluster( metaStore, newNamedCluster );
        FileUtils.copyDirectory( sourceClusterConfigDir, newClusterConfigDir );
        if ( !nc.getShimIdentifier().equals( newNamedCluster.getShimIdentifier() ) ) {
          addConfigProperties( newNamedCluster );
        }
      }
    } catch ( Exception e ) {
      commonDialogFactory.createErrorDialog( spoon.getShell(),
        BaseMessages.getString( PKG, SPOON_DIALOG_ERROR_SAVING_NAMED_CLUSTER_TITLE ),
        BaseMessages.getString( PKG, SPOON_DIALOG_ERROR_SAVING_NAMED_CLUSTER_MESSAGE, nc.getName() ), e );
      spoon.refreshTree();
      return;
    }
    spoon.refreshTree( STRING_NAMED_CLUSTERS );
  }

  public void delNamedCluster( IMetaStore metaStore, NamedCluster namedCluster ) {
    if ( metaStore == null ) {
      metaStore = spoon.getMetaStore();
    }
    deleteNamedCluster( metaStore, namedCluster );
    spoon.refreshTree( STRING_NAMED_CLUSTERS );
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

    if ( result == null ) {
      return null;
    }

    // Create the new cluster
    saveNamedCluster( metaStore, namedClusterDialogImpl.getNamedCluster() );

    if ( namedCluster.getName() == namedClusterDialogImpl.getNamedCluster().getName() ) {
      return namedClusterDialogImpl.getNamedCluster().getName();
    }

    XmlMetaStore xmlMetaStore;
    try {
      xmlMetaStore = getXmlMetastore( metaStore );
    } catch ( MetaStoreException ex ) {
      xmlMetaStore = null;
    }

    // Rename the configuration folder to the new name.
    File source = new File( getNamedClusterConfigsRootDir( xmlMetaStore ) + "/" + namedCluster.getName() );
    File destination = new File(
      getNamedClusterConfigsRootDir( xmlMetaStore ) + "/" + namedClusterDialogImpl.getNamedCluster().getName() );

    try {
      FileUtils.copyDirectory( source, destination );
    } catch ( IOException ex ) {

    }

    // Delete the old named cluster.
    deleteNamedCluster( metaStore, namedCluster );

    // If the user changed the shim, create a new config.properties file that corresponds to that shim.
    String shimIdentifier = namedClusterDialogImpl.getNamedCluster().getShimIdentifier();
    if ( !namedCluster.getShimIdentifier().equals( shimIdentifier ) ) {
      try {
        addConfigProperties( namedClusterDialogImpl.getNamedCluster() );
      } catch ( Exception e ) {
        // Do nothing.
      }
    }

    spoon.refreshTree( STRING_NAMED_CLUSTERS );
    if ( namedClusterDialogImpl.getNamedCluster() != null ) {
      return namedClusterDialogImpl.getNamedCluster().getName();
    }

    return null;
  }

  private XmlMetaStore getXmlMetastore( IMetaStore metaStore ) throws MetaStoreException {
    XmlMetaStore xmlMetaStore = null;

    if ( metaStore instanceof DelegatingMetaStore ) {
      IMetaStore activeMetastore = ( (DelegatingMetaStore) metaStore ).getActiveMetaStore();
      if ( activeMetastore instanceof XmlMetaStore ) {
        xmlMetaStore = (XmlMetaStore) activeMetastore;
      }
    } else if ( metaStore instanceof XmlMetaStore ) {
      xmlMetaStore = (XmlMetaStore) metaStore;
    }

    return xmlMetaStore;
  }

  private String getNamedClusterConfigsRootDir( XmlMetaStore metaStore ) {
    return System.getProperty( "user.home" ) + File.separator + ".pentaho" + File.separator + "metastore"
      + File.separator + "pentaho" + File.separator + "NamedCluster" + File.separator + "Configs";
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

      try {
        saveNamedCluster( metaStore, nc );
        addConfigProperties( nc );
      } catch ( Exception e ) {
        commonDialogFactory.createErrorDialog( spoon.getShell(),
          BaseMessages.getString( PKG, SPOON_DIALOG_ERROR_SAVING_NAMED_CLUSTER_TITLE ),
          BaseMessages.getString( PKG, SPOON_DIALOG_ERROR_SAVING_NAMED_CLUSTER_MESSAGE, nc.getName() ), e );
        spoon.refreshTree();
        return nc.getName();
      }

      spoon.refreshTree( STRING_NAMED_CLUSTERS );
      return nc.getName();
    }
    return null;
  }

  private void addConfigProperties( NamedCluster namedCluster ) throws Exception {
    Path clusterConfigDirPath = Paths.get( getNamedClusterConfigsRootDir( null ) + "/" + namedCluster.getName() );
    Path configPropertiesPath =
      Paths.get( getNamedClusterConfigsRootDir( null ) + "/" + namedCluster.getName() + "/" + "config.properties" );
    Files.createDirectories( clusterConfigDirPath );
    String sampleConfigProperties = namedCluster.getShimIdentifier() + "sampleconfig.properties";
    InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream( sampleConfigProperties );
    if ( inputStream != null ) {
      Files.copy( inputStream, configPropertiesPath, StandardCopyOption.REPLACE_EXISTING );
    }
  }

  private void deleteNamedCluster( IMetaStore metaStore, NamedCluster namedCluster ) {
    try {
      if ( namedClusterService.read( namedCluster.getName(), metaStore ) != null ) {
        namedClusterService.delete( namedCluster.getName(), metaStore );
        XmlMetaStore xmlMetaStore = getXmlMetastore( metaStore );
        if ( xmlMetaStore != null ) {
          String path = getNamedClusterConfigsRootDir( xmlMetaStore ) + "/" + namedCluster.getName();
          try {
            FileUtils.deleteDirectory( new File( path ) );
          } catch ( IOException e ) {
            // Do nothing. The config directory will be orphaned but functionality will not be impacted.
          }
        }
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
