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



package org.pentaho.runtime.test.result;

import java.util.List;

/**
 * Created by bryan on 8/26/15.
 */
public interface RuntimeTestResultSummary {
  RuntimeTestResultEntry getOverallStatusEntry();

  List<RuntimeTestResultEntry> getRuntimeTestResultEntries();
}
