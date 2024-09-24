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
