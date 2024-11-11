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


package org.pentaho.runtime.test.i18n.impl;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by bryan on 8/27/15.
 */
public class BaseMessagesMessageGetterImplTest {
  private BaseMessagesMessageGetterImpl baseMessagesMessageGetter;

  @Before
  public void setup() {
    baseMessagesMessageGetter = new BaseMessagesMessageGetterImpl( BaseMessagesMessageGetterFactoryImplTest.class );
  }

  @Test
  public void testGetMesssage() {
    String message = "message";
    String expected = "!" + message + "!";
    assertEquals( expected, baseMessagesMessageGetter.getMessage( message ) );
    assertEquals( expected, baseMessagesMessageGetter.getMessage( message, "testParam" ) );
  }
}
