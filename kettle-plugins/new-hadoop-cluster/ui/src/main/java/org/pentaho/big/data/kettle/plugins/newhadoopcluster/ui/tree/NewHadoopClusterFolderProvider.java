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

package org.pentaho.big.data.kettle.plugins.newhadoopcluster.ui.tree;

import org.eclipse.swt.graphics.Image;
import org.pentaho.di.base.AbstractMeta;
import org.pentaho.di.connections.ConnectionManager;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.tree.TreeNode;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.tree.TreeFolderProvider;
import org.pentaho.di.ui.util.SwtSvgImageUtil;
import org.pentaho.osgi.metastore.locator.api.MetastoreLocator;

import java.util.function.Supplier;

/**
 * Created by bmorrise on 7/6/18.
 */
public class NewHadoopClusterFolderProvider extends TreeFolderProvider {

  private static final Class<?> PKG = NewHadoopClusterFolderProvider.class;
  public static final String STRING_NEW_HADOOP_CLUSTER = BaseMessages.getString( PKG, "NewHadoopClusterTree.Title" );
  private ConnectionManager connectionManager;
  private Supplier<Spoon> spoonSupplier = Spoon::getInstance;

  public NewHadoopClusterFolderProvider( MetastoreLocator metastoreLocator ) {
    this.connectionManager = ConnectionManager.getInstance();
    connectionManager.setMetastoreSupplier( metastoreLocator::getMetastore );
  }

  @Override
  public void refresh( AbstractMeta meta, TreeNode treeNode, String filter ) {
    for ( String name : connectionManager.getNames() ) {
      if ( !filterMatch( name, filter ) ) {
        continue;
      }
      super.createTreeNode( treeNode, name, GUIResource.getInstance().getImageSlaveTree() );
    }
  }

  @Override
  public String getTitle() {
    return STRING_NEW_HADOOP_CLUSTER;
  }
}
