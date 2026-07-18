/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/



package org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.endpoints;

public class Test {

  private String testName = "";
  private String testStatus = "";
  private boolean isTestActive = false;

  public Test() {
  }

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
