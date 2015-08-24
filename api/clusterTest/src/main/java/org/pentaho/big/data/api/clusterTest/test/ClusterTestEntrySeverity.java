/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

package org.pentaho.big.data.api.clusterTest.test;

import java.util.Collection;

/**
 * Created by bryan on 8/11/15.
 */
public enum ClusterTestEntrySeverity {
  DEBUG, INFO, WARNING, SKIPPED, ERROR, FATAL;

  public static ClusterTestEntrySeverity maxSeverityEntry( Collection<ClusterTestResultEntry> clusterTestResultEntries ) {
    ClusterTestEntrySeverity maxSeverity = null;
    for ( ClusterTestResultEntry clusterTestResultEntry : clusterTestResultEntries ) {
      ClusterTestEntrySeverity severity = clusterTestResultEntry.getSeverity();
      if ( maxSeverity == null || ( severity != null && severity.ordinal() > maxSeverity.ordinal() ) ) {
        maxSeverity = severity;
      }
    }
    return maxSeverity;
  }

  public static ClusterTestEntrySeverity maxSeverityResult( Collection<ClusterTestResult> clusterTestResults ) {
    ClusterTestEntrySeverity maxSeverity = null;
    for ( ClusterTestResult clusterTestResult : clusterTestResults ) {
      ClusterTestEntrySeverity severity = clusterTestResult.getMaxSeverity();
      if ( maxSeverity == null || ( severity != null && severity.ordinal() > maxSeverity.ordinal() ) ) {
        maxSeverity = severity;
      }
    }
    return maxSeverity;
  }
}
