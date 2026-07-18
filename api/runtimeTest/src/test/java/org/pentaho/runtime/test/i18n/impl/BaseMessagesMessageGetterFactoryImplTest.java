/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/



package org.pentaho.runtime.test.i18n.impl;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Created by bryan on 8/27/15.
 */
public class BaseMessagesMessageGetterFactoryImplTest {
  private BaseMessagesMessageGetterFactoryImpl baseMessagesMessageGetterFactory;

  @Before
  public void setup() {
    baseMessagesMessageGetterFactory = new BaseMessagesMessageGetterFactoryImpl();
  }

  @Test
  public void testCreate() {
    assertTrue( baseMessagesMessageGetterFactory
      .create( BaseMessagesMessageGetterFactoryImplTest.class ) instanceof BaseMessagesMessageGetterImpl );
  }
}
