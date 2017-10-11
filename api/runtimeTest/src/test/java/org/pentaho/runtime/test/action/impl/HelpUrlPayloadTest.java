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
