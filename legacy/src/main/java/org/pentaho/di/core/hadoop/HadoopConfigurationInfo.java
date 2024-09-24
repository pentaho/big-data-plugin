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

/**
 * Created by bryan on 8/10/15.
 */
public class HadoopConfigurationInfo {
  private final String id;
  private final String name;
  private final boolean isActive;
  private final boolean willBeActiveAfterRestart;

  public HadoopConfigurationInfo( String id, String name, boolean isActive, boolean willBeActiveAfterRestart ) {
    this.id = id;
    this.name = name;
    this.isActive = isActive;
    this.willBeActiveAfterRestart = willBeActiveAfterRestart;
  }

  public String getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public boolean isActive() {
    return isActive;
  }

  public boolean isWillBeActiveAfterRestart() {
    return willBeActiveAfterRestart;
  }
}
