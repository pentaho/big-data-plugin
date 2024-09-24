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

package org.pentaho.runtime.test.module;

import org.pentaho.runtime.test.RuntimeTest;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;
import org.pentaho.runtime.test.result.RuntimeTestResult;

import java.util.List;
import java.util.Set;

/**
 * Created by bryan on 8/11/15.
 */
public interface RuntimeTestModuleResults {
  String getName();

  List<RuntimeTestResult> getRuntimeTestResults();

  Set<RuntimeTest> getRunningTests();

  Set<RuntimeTest> getOutstandingTests();

  RuntimeTestEntrySeverity getMaxSeverity();
}
