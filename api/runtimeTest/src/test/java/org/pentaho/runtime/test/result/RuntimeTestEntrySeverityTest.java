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

package org.pentaho.runtime.test.result;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 8/27/15.
 */
public class RuntimeTestEntrySeverityTest {
  @Test
  public void testMaxSeverityResult() {
    RuntimeTestResult runtimeTestResult1 = mock( RuntimeTestResult.class );
    RuntimeTestResult runtimeTestResult2 = mock( RuntimeTestResult.class );
    RuntimeTestResult runtimeTestResult3 = mock( RuntimeTestResult.class );
    RuntimeTestResultEntry runtimeTestResultEntry1 = mock( RuntimeTestResultEntry.class );
    RuntimeTestResultEntry runtimeTestResultEntry2 = mock( RuntimeTestResultEntry.class );
    RuntimeTestResultEntry runtimeTestResultEntry3 = mock( RuntimeTestResultEntry.class );

    when( runtimeTestResult1.getOverallStatusEntry() ).thenReturn( runtimeTestResultEntry1 );
    when( runtimeTestResult2.getOverallStatusEntry() ).thenReturn( runtimeTestResultEntry2 );
    when( runtimeTestResult3.getOverallStatusEntry() ).thenReturn( runtimeTestResultEntry3 );

    when( runtimeTestResultEntry1.getSeverity() ).thenReturn( RuntimeTestEntrySeverity.INFO );
    when( runtimeTestResultEntry2.getSeverity() ).thenReturn( RuntimeTestEntrySeverity.FATAL );
    when( runtimeTestResultEntry3.getSeverity() ).thenReturn( null );

    when( runtimeTestResult1.isDone() ).thenReturn( true );
    when( runtimeTestResult2.isDone() ).thenReturn( false ).thenReturn( true );
    when( runtimeTestResult3.isDone() ).thenReturn( true );

    assertEquals( RuntimeTestEntrySeverity.INFO, RuntimeTestEntrySeverity
      .maxSeverityResult( Arrays.asList( runtimeTestResult1, runtimeTestResult2, runtimeTestResult3 ) ) );
    assertEquals( RuntimeTestEntrySeverity.FATAL, RuntimeTestEntrySeverity
      .maxSeverityResult( Arrays.asList( runtimeTestResult1, runtimeTestResult2, runtimeTestResult3 ) ) );
  }

  @Test
  public void testValuesAndValueOf() {
    for ( RuntimeTestEntrySeverity runtimeTestEntrySeverity : RuntimeTestEntrySeverity.values() ) {
      assertEquals( runtimeTestEntrySeverity, RuntimeTestEntrySeverity.valueOf( runtimeTestEntrySeverity.name() ) );
    }
  }
}
