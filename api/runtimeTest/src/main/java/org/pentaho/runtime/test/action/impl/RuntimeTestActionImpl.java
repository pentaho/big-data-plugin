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


package org.pentaho.runtime.test.action.impl;

import org.pentaho.runtime.test.action.RuntimeTestAction;
import org.pentaho.runtime.test.action.RuntimeTestActionPayload;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;

/**
 * Created by bryan on 9/9/15.
 */
public class RuntimeTestActionImpl implements RuntimeTestAction {
  private final String name;
  private final String description;
  private final RuntimeTestEntrySeverity severity;
  private final RuntimeTestActionPayload payload;

  public RuntimeTestActionImpl( String name, String description, RuntimeTestEntrySeverity severity,
                                RuntimeTestActionPayload payload ) {
    this.name = name;
    this.description = description;
    this.severity = severity;
    this.payload = payload;
  }

  @Override public String getName() {
    return name;
  }

  @Override public String getDescription() {
    return description;
  }

  @Override public RuntimeTestEntrySeverity getSeverity() {
    return severity;
  }

  @Override public RuntimeTestActionPayload getPayload() {
    return payload;
  }
}
