/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

import org.pentaho.di.core.annotations.LifecyclePlugin;
import org.pentaho.di.core.gui.GUIOption;
import org.pentaho.di.core.lifecycle.LifeEventHandler;
import org.pentaho.di.core.lifecycle.LifecycleException;
import org.pentaho.di.core.lifecycle.LifecycleListener;
import org.pentaho.di.core.util.ExecutorUtil;
import org.pentaho.di.ui.core.namedcluster.NamedClusterUIHelper;
import org.pentaho.di.ui.spoon.Spoon;

import java.util.function.Supplier;

@LifecyclePlugin( id = "HadoopSpoonPlugin", name = "Hadoop Spoon Plugin" )
public class HadoopSpoonPlugin implements LifecycleListener, GUIOption<Object> {
  public static final String PLUGIN_ID = "HadoopSpoonPlugin";
  @SuppressWarnings( "unused" )
  private static Class<?> PKG = HadoopSpoonPlugin.class;

  private Supplier<Spoon> spoonSupplier = Spoon::getInstance;
  public static final String HDFS_SCHEME = "hdfs";
  public static final String HDFS_SCHEME_DISPLAY_NAME = "HDFS";
  public static final String MAPRFS_SCHEME = "maprfs";
  public static final String MAPRFS_SCHEME_DISPLAY_NAME = "MapRFS";

  public void onStart( LifeEventHandler arg0 ) throws LifecycleException {
    ExecutorUtil.getExecutor().submit( new Runnable() {
      @Override public void run() {
        // Block until factory is available before we get onto display thread
        NamedClusterUIHelper.getNamedClusterUIFactory();
      }
    } );
    Spoon spoon = spoonSupplier.get();
    if ( spoon != null ) {
      spoon.getTreeManager().addTreeProvider( Spoon.STRING_TRANSFORMATIONS, new HadoopClusterFolderProvider() );
      spoon.getTreeManager().addTreeProvider( Spoon.STRING_JOBS, new HadoopClusterFolderProvider() );
    }
  }

  public void onExit( LifeEventHandler arg0 ) throws LifecycleException {
  }

  public String getLabelText() {
    return null;
  }

  public Object getLastValue() {
    return null;
  }

  public DisplayType getType() {
    return null;
  }

  public void setValue( Object value ) {
  }
}
