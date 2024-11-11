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

import java.util.List;

public interface Category {

  public List<Test> getTests();

  public String getCategoryName();

  public void setCategoryName( String categoryName );

  public void setTests( List<Test> tests );

  public String getCategoryStatus();

  public void setCategoryStatus( String categoryStatus );

  public boolean isCategoryActive();

  public void setCategoryActive( boolean categoryActive );

  public void addTest( Test test );
}
