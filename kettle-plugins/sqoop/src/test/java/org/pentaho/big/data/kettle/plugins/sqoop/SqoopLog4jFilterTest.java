/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2021 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.big.data.kettle.plugins.sqoop;

import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.util.ReadOnlyStringMap;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SqoopLog4jFilterTest {

  @Test
  public void decide() {
    String goodLog = "goodLog";
    String badLog = "badLog";
    LogEvent goodEvent = mock( LogEvent.class );
    ReadOnlyStringMap goodContextData = mock( ReadOnlyStringMap.class );
    when( goodContextData.getValue( "logChannelId" ) ).thenReturn( goodLog );
    when( goodEvent.getContextData() ).thenReturn( goodContextData );
    LogEvent badEvent = mock( LogEvent.class );
    ReadOnlyStringMap badContextData = mock( ReadOnlyStringMap.class );
    when( badContextData.getValue( "logChannelId" ) ).thenReturn( badLog );
    when( badEvent.getContextData() ).thenReturn( badContextData );
    Filter f = new SqoopLog4jFilter( goodLog );
    assertEquals( Filter.Result.NEUTRAL, f.filter( goodEvent ) );
    assertEquals( Filter.Result.DENY, f.filter( badEvent ) );
  }
}
