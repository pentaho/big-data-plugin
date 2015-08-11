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

package org.pentaho.big.data.api.clusterTest;

import org.pentaho.big.data.api.clusterTest.test.ClusterTestEntrySeverity;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResultEntry;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by bryan on 8/21/15.
 */
public class ClusterTestEntryUtil {
  public static ClusterTestResultEntry expectOneEntry( List<ClusterTestResultEntry> clusterTestResultEntries ) {
    assertNotNull( clusterTestResultEntries );
    assertEquals( 1, clusterTestResultEntries.size() );
    return clusterTestResultEntries.get( 0 );
  }

  public static void verifyClusterTestResultEntry( ClusterTestResultEntry clusterTestResultEntry,
                                                   ClusterTestEntrySeverity severity, String desc, String message ) {
    verifyClusterTestResultEntry( clusterTestResultEntry, severity, desc, message, null );
  }

  public static Throwable verifyClusterTestResultEntry( ClusterTestResultEntry clusterTestResultEntry,
                                                        ClusterTestEntrySeverity severity, String desc, String message,
                                                        Class<?> exceptionClass ) {
    assertNotNull( clusterTestResultEntry );
    assertEquals( severity, clusterTestResultEntry.getSeverity() );
    assertEquals( desc, clusterTestResultEntry.getDescription() );
    assertEquals( message, clusterTestResultEntry.getMessage() );
    Throwable clusterTestResultEntryException = clusterTestResultEntry.getException();
    if ( exceptionClass == null ) {
      assertNull( clusterTestResultEntryException );
    } else {
      assertTrue( "expected exception of type " + exceptionClass,
        exceptionClass.isInstance( clusterTestResultEntryException ) );
    }
    return clusterTestResultEntryException;
  }
}
