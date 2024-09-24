/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.runtime.test;

import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;
import org.pentaho.runtime.test.result.RuntimeTestResultEntry;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by bryan on 8/21/15.
 */
public class RuntimeTestEntryUtil {
  public static RuntimeTestResultEntry expectOneEntry( List<RuntimeTestResultEntry> runtimeTestResultEntries ) {
    assertNotNull( runtimeTestResultEntries );
    assertEquals( 1, runtimeTestResultEntries.size() );
    return runtimeTestResultEntries.get( 0 );
  }

  public static void verifyRuntimeTestResultEntry( RuntimeTestResultEntry runtimeTestResultEntry,
                                                   RuntimeTestEntrySeverity severity, String desc, String message ) {
    verifyRuntimeTestResultEntry( runtimeTestResultEntry, severity, desc, message, null );
  }

  public static Throwable verifyRuntimeTestResultEntry( RuntimeTestResultEntry runtimeTestResultEntry,
                                                        RuntimeTestEntrySeverity severity, String desc, String message,
                                                        Class<?> exceptionClass ) {
    assertNotNull( runtimeTestResultEntry );
    assertEquals( severity, runtimeTestResultEntry.getSeverity() );
    assertEquals( desc, runtimeTestResultEntry.getDescription() );
    assertEquals( message, runtimeTestResultEntry.getMessage() );
    Throwable runtimeTestResultEntryException = runtimeTestResultEntry.getException();
    if ( exceptionClass == null ) {
      assertNull( runtimeTestResultEntryException );
    } else {
      assertTrue( "expected exception of type " + exceptionClass,
        exceptionClass.isInstance( runtimeTestResultEntryException ) );
    }
    return runtimeTestResultEntryException;
  }
}
