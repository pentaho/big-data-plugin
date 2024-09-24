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

package org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.endpoints;

import java.util.ArrayList;
import java.util.List;

public class TestCategory implements Category {

  private List<Test> tests = new ArrayList<>();
  private String categoryName = "";
  private String categoryStatus = "";
  private boolean isCategoryActive = false;

  public TestCategory( String name ) {
    setCategoryName( name );
  }

  public List<Test> getTests() {
    return tests;
  }

  public String getCategoryName() {
    return categoryName;
  }

  public void setCategoryName( String categoryName ) {
    this.categoryName = categoryName;
  }

  public void setTests( List<Test> tests ) {
    this.tests = tests;
  }

  public String getCategoryStatus() {
    return categoryStatus;
  }

  public void setCategoryStatus( String categoryStatus ) {
    this.categoryStatus = categoryStatus;
  }

  public boolean isCategoryActive() {
    return isCategoryActive;
  }

  public void setCategoryActive( boolean categoryActive ) {
    isCategoryActive = categoryActive;
  }

  public void addTest( Test test ) {
    this.tests.add( test );
  }
}
