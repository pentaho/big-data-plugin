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