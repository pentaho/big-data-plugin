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

package org.pentaho.runtime.test.result;

import java.util.Collection;

/**
 * Created by bryan on 8/11/15.
 */
public enum RuntimeTestEntrySeverity {
  DEBUG, INFO, WARNING, SKIPPED, ERROR, FATAL;

  public static RuntimeTestEntrySeverity maxSeverityResult( Collection<RuntimeTestResult> runtimeTestResults ) {
    RuntimeTestEntrySeverity maxSeverity = null;
    for ( RuntimeTestResult runtimeTestResult : runtimeTestResults ) {
      if ( runtimeTestResult.isDone() ) {
        RuntimeTestEntrySeverity severity = runtimeTestResult.getOverallStatusEntry().getSeverity();
        if ( maxSeverity == null || ( severity != null && severity.ordinal() > maxSeverity.ordinal() ) ) {
          maxSeverity = severity;
        }
      }
    }
    return maxSeverity;
  }
}
