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

package org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.tree;

import org.pentaho.di.base.AbstractMeta;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.widget.tree.TreeNode;
import org.pentaho.di.ui.spoon.tree.TreeFolderProvider;

public class HadoopClusterFolderProvider extends TreeFolderProvider {

  private static final Class<?> PKG = HadoopClusterFolderProvider.class;
  public static final String STRING_NEW_HADOOP_CLUSTER = BaseMessages.getString( PKG, "HadoopClusterTree.Title" );

  @Override
  public void refresh( AbstractMeta meta, TreeNode treeNode, String filter ) {
  }

  @Override
  public String getTitle() {
    return STRING_NEW_HADOOP_CLUSTER;
  }
}
