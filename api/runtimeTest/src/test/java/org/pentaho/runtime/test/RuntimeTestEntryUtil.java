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
