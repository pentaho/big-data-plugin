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



package org.pentaho.runtime.test;

/**
 * Created by bryan on 8/11/15.
 */
public interface RuntimeTester {
  void runtimeTest( Object objectUnderTest, RuntimeTestProgressCallback runtimeTestProgressCallback );
  void addRuntimeTest( RuntimeTest test );
}
