/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.endpoints;

public class Test {

  private String testName = "";
  private String testStatus = "";
  private boolean isTestActive = false;

  public Test( String name ) {
    setTestName( name );
  }

  public String getTestName() {
    return testName;
  }

  public void setTestName( String testName ) {
    this.testName = testName;
  }

  public String getTestStatus() {
    return testStatus;
  }

  public void setTestStatus( String testStatus ) {
    this.testStatus = testStatus;
  }

  public boolean isTestActive() {
    return isTestActive;
  }

  public void setTestActive( boolean testActive ) {
    isTestActive = testActive;
  }
}
