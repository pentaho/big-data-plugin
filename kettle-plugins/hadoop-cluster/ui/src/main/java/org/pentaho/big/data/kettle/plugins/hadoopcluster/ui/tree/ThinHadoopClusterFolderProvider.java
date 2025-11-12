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

import org.eclipse.swt.graphics.Image;
import org.pentaho.big.data.api.services.BigDataServicesHelper;
import org.pentaho.di.base.AbstractMeta;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.widget.tree.TreeNode;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.tree.TreeFolderProvider;
import org.pentaho.di.ui.util.SwtSvgImageUtil;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.Optional;

public class ThinHadoopClusterFolderProvider extends TreeFolderProvider {

  public static final String STRING_NEW_HADOOP_CLUSTER =
    BaseMessages.getString( ThinHadoopClusterFolderProvider.class, "HadoopClusterTree.Title" );
  private static Class<?> PKG = Spoon.class;
  private Supplier<Spoon> spoonSupplier = Spoon::getInstance;
  private NamedClusterService namedClusterService;

  @Override
  public void refresh( Optional<AbstractMeta> meta, TreeNode treeNode, String filter ) {

    if ( getNamedClusterService() != null ) {
      List<NamedCluster> namedClusters = null;
      List<MetaStoreException> exceptionList = new ArrayList<>();
      try {
        namedClusters = getNamedClusterService().list( Spoon.getInstance().getMetaStore(), exceptionList );
        for ( MetaStoreException e : exceptionList ) {
          new ErrorDialog( Spoon.getInstance().getShell(), BaseMessages.getString( PKG, "Spoon.ErrorDialog.Title" ),
            BaseMessages.getString( PKG, "Spoon.ErrorDialog.ErrorFetchingFromRepo.NamedCluster" ), e );
        }
      } catch ( MetaStoreException e ) {
        new ErrorDialog( Spoon.getInstance().getShell(), BaseMessages.getString( PKG, "Spoon.ErrorDialog.Title" ),
          BaseMessages.getString( PKG, "Spoon.ErrorDialog.ErrorFetchingFromRepo.NamedCluster" ), e );
        return;
      }

      for ( NamedCluster namedCluster : namedClusters ) {
        if ( !filterMatch( namedCluster.getName(), filter ) ) {
          continue;
        }
        createTreeNode( treeNode, namedCluster.getName(), getHadoopClusterImage() );
      }
    }
  }

  @Override
  public String getTitle() {
    return STRING_NEW_HADOOP_CLUSTER;
  }

  @Override
  public Class getType() {
    return NamedCluster.class;
  }

  private Image getHadoopClusterImage() {
    return SwtSvgImageUtil
      .getImage( spoonSupplier.get().getShell().getDisplay(), getClass().getClassLoader(), "images/hadoop_clusters.svg",
        ConstUI.ICON_SIZE, ConstUI.ICON_SIZE );
  }

  private NamedClusterService getNamedClusterService() {
    if ( namedClusterService == null ) {
      namedClusterService = BigDataServicesHelper.getNamedClusterService();
    }
    return namedClusterService;
  }
}
