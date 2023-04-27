/*! ******************************************************************************
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

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by bryan on 8/14/15.
 */
public class HadoopConfigurationInfoTest {
  private String id;
  private String name;
  private boolean isActive;
  private boolean willBeActiveAfterRestart;
  private HadoopConfigurationInfo hadoopConfigurationInfo;

  @Before
  public void setup() {
    id = "testId";
    name = "testName";
    isActive = true;
    willBeActiveAfterRestart = true;
    createHadoopConfigurationInfo();
  }

  private void createHadoopConfigurationInfo() {
    hadoopConfigurationInfo = new HadoopConfigurationInfo( id, name, isActive, willBeActiveAfterRestart );
  }

  @Test
  public void testGetId() {
    assertEquals( id, hadoopConfigurationInfo.getId() );
  }

  @Test
  public void testGetName() {
    assertEquals( name, hadoopConfigurationInfo.getName() );
  }

  @Test
  public void testIsActive() {
    assertTrue( hadoopConfigurationInfo.isActive() );
    isActive = false;
    createHadoopConfigurationInfo();
    assertFalse( hadoopConfigurationInfo.isActive() );
  }

  @Test
  public void testWillBeActiveAfterRestart() {
    assertTrue( hadoopConfigurationInfo.isWillBeActiveAfterRestart() );
    willBeActiveAfterRestart = false;
    createHadoopConfigurationInfo();
    assertFalse( hadoopConfigurationInfo.isWillBeActiveAfterRestart() );
  }
}
