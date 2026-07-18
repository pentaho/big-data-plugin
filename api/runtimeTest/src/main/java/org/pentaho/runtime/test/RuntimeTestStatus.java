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

import org.pentaho.runtime.test.module.RuntimeTestModuleResults;

import java.util.List;

/**
 * Created by bryan on 8/18/15.
 */
public interface RuntimeTestStatus {
  List<RuntimeTestModuleResults> getModuleResults();

  int getTestsDone();

  int getTestsRunning();

  int getTestsOutstanding();

  boolean isDone();
}
