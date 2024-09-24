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

package org.pentaho.runtime.test.action.impl;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.runtime.test.TestMessageGetterFactory;
import org.pentaho.runtime.test.i18n.MessageGetter;

import static org.junit.Assert.assertEquals;

/**
 * Created by bryan on 9/10/15.
 */
public class HelpUrlPayloadTest {
  private MessageGetter messageGetter;
  private String title;
  private String header;
  private String url;
  private HelpUrlPayload helpUrlPayload;

  @Before
  public void setup() {
    TestMessageGetterFactory messageGetterFactory = new TestMessageGetterFactory();
    messageGetter = messageGetterFactory.create( HelpUrlPayload.class );
    title = "title";
    header = "header";
    url = "url";
    helpUrlPayload = new HelpUrlPayload( messageGetterFactory, title, header, url );
  }

  @Test
  public void testGetTitle() {
    assertEquals( title, helpUrlPayload.getTitle() );
  }

  @Test
  public void testGetHeader() {
    assertEquals( header, helpUrlPayload.getHeader() );
  }

  @Test
  public void testGetUrl() {
    assertEquals( url, helpUrlPayload.getUrl() );
  }

  @Test
  public void testGetMessage() {
    assertEquals( messageGetter.getMessage( HelpUrlPayload.HELP_URL_PAYLOAD_MESSAGE, url ),
      helpUrlPayload.getMessage() );
  }
}
