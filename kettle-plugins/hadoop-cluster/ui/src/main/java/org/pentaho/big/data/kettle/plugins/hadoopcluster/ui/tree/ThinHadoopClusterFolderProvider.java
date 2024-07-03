/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2019-2020 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.tree;

import org.eclipse.swt.graphics.Image;
import org.pentaho.di.base.AbstractMeta;
import org.pentaho.di.core.namedcluster.NamedClusterManager;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.widget.tree.TreeNode;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.tree.TreeFolderProvider;
import org.pentaho.di.ui.util.SwtSvgImageUtil;
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

  @Override
  public void refresh( Optional<AbstractMeta> meta, TreeNode treeNode, String filter ) {
    List<NamedCluster> namedClusters;
    List<MetaStoreException> exceptionList = new ArrayList<>();
    try {
      namedClusters = NamedClusterManager.getInstance().list( Spoon.getInstance().getMetaStore(), exceptionList );
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

  @Override
  public String getTitle() {
    return STRING_NEW_HADOOP_CLUSTER;
  }

  private Image getHadoopClusterImage() {
    return SwtSvgImageUtil
      .getImage( spoonSupplier.get().getShell().getDisplay(), getClass().getClassLoader(), "images/hadoop_clusters.svg",
        ConstUI.ICON_SIZE, ConstUI.ICON_SIZE );
  }
}
