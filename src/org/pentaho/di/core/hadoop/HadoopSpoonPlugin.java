/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
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
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.vfs.hadoopvfsfilechooserdialog.HadoopVfsFileChooserDialog;
import org.pentaho.vfs.ui.VfsFileChooserDialog;

@LifecyclePlugin( id = "HadoopSpoonPlugin", name = "Hadoop Spoon Plugin" )
public class HadoopSpoonPlugin implements LifecycleListener, GUIOption<Object> {
  public static final String PLUGIN_ID = "HadoopSpoonPlugin";
  @SuppressWarnings( "unused" )
  private static Class<?> PKG = HadoopSpoonPlugin.class;

  public static final String HDFS_SCHEME = "hdfs";
  public static final String HDFS_SCHEME_DISPLAY_NAME = "HDFS";
  public static final String MAPRFS_SCHEME = "maprfs";
  public static final String MAPRFS_SCHEME_DISPLAY_NAME = "MapRFS";

  public void onStart( LifeEventHandler arg0 ) throws LifecycleException {
    VfsFileChooserDialog dialog = Spoon.getInstance().getVfsFileChooserDialog( null, null );
    dialog.addVFSUIPanel( new HadoopVfsFileChooserDialog( HDFS_SCHEME, HDFS_SCHEME_DISPLAY_NAME, dialog, null, null ) );
    dialog
        .addVFSUIPanel( new HadoopVfsFileChooserDialog(
            MAPRFS_SCHEME, MAPRFS_SCHEME_DISPLAY_NAME, dialog, null, null ) );
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
