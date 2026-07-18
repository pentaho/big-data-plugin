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



package org.pentaho.runtime.test.action;

import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;

/**
 * Created by bryan on 9/8/15.
 */
public interface RuntimeTestAction {
  String getName();
  String getDescription();
  RuntimeTestEntrySeverity getSeverity();
  RuntimeTestActionPayload getPayload();
}
