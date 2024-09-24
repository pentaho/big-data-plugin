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

package org.pentaho.runtime.test;

import org.pentaho.runtime.test.result.RuntimeTestResultSummary;

import java.util.Set;

/**
 * Created by bryan on 8/11/15.
 */
public interface RuntimeTest {
  boolean accepts( Object objectUnderTest );

  String getModule();

  String getId();

  String getName();

  boolean isConfigInitTest();

  Set<String> getDependencies();

  RuntimeTestResultSummary runTest( Object objectUnderTest );
}
