/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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
