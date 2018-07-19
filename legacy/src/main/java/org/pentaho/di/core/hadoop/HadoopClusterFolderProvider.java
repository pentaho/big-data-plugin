/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.core.hadoop;

import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.Display;
import org.pentaho.di.base.AbstractMeta;
import org.pentaho.di.core.SwtUniversalImage;
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

import java.util.List;

/**
 * Created by bmorrise on 7/6/18.
 */
public class HadoopClusterFolderProvider extends TreeFolderProvider {

  public static final String STRING_NAMED_CLUSTERS =
          BaseMessages.getString( HadoopClusterViewTreeExtension.class, "NamedClusterDialog.STRING_NAMED_CLUSTERS" );
  private static Class<?> PKG = Spoon.class;

  @Override
  public void refresh( AbstractMeta meta, TreeNode treeNode, String filter ) {
    List<NamedCluster> namedClusters;
    try {
      namedClusters = NamedClusterManager.getInstance().list( Spoon.getInstance().metaStore );
    } catch ( MetaStoreException e ) {
      new ErrorDialog( Spoon.getInstance().getShell(), BaseMessages.getString( PKG, "Spoon.ErrorDialog.Title" ),
              BaseMessages.getString( PKG, "Spoon.ErrorDialog.ErrorFetchingFromRepo.NamedCluster" ), e );

      return;
    }

    for ( NamedCluster namedCluster : namedClusters ) {
      if ( !filterMatch( namedCluster.getName(), filter ) ) {
        continue;
      }
      createTreeNode( treeNode, namedCluster.getName(), getHadoopClusterImage( Spoon.getInstance().getDisplay() ) );
    }
  }

  @Override
  public String getTitle() {
    return STRING_NAMED_CLUSTERS;
  }

  private Image getHadoopClusterImage( Display display ) {
    final SwtUniversalImage swtImage =
            SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), "hadoop_clusters.svg" );
    Image image = swtImage.getAsBitmapForSize( display, ConstUI.MEDIUM_ICON_SIZE, ConstUI.MEDIUM_ICON_SIZE );
    display.addListener( SWT.Dispose, event -> swtImage.dispose() );
    return image;
  }
}
