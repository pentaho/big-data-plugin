/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package org.pentaho.di.core.hadoop;

import org.pentaho.di.core.annotations.LifecyclePlugin;
import org.pentaho.di.core.gui.GUIOption;
import org.pentaho.di.core.lifecycle.LifeEventHandler;
import org.pentaho.di.core.lifecycle.LifecycleListener;

@LifecyclePlugin( id = "HadoopSpoonPlugin", name = "Hadoop Spoon Plugin" )
public class HadoopSpoonPlugin implements LifecycleListener, GUIOption<Object> {
  public static final String PLUGIN_ID = "HadoopSpoonPlugin";

  public static final String HDFS_SCHEME = "hdfs";
  public static final String MAPRFS_SCHEME = "maprfs";

  public void onStart( LifeEventHandler arg0 ) {
    //TODO: This class should be removed as a LifecyclePlugin and no longer implement LifecycleListener once dependencies are cleaned up
  }

  public void onExit( LifeEventHandler arg0 ) {
    //TODO: This class should be removed as a LifecyclePlugin and no longer implement LifecycleListener once dependencies are cleaned up
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
