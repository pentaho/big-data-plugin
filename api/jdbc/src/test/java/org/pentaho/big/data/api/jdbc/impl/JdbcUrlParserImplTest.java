/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

package org.pentaho.big.data.api.jdbc.impl;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.osgi.metastore.locator.api.MetastoreLocator;

import java.net.URISyntaxException;

import static org.junit.Assert.assertTrue;

/**
 * Created by bryan on 4/14/16.
 */
@RunWith( MockitoJUnitRunner.class )
public class JdbcUrlParserImplTest {
  @Mock NamedClusterService namedClusterService;
  @Mock MetastoreLocator metastoreLocator;
  JdbcUrlParserImpl jdbcUrlParser;

  @Before
  public void setup() {
    jdbcUrlParser = new JdbcUrlParserImpl( namedClusterService, metastoreLocator );
  }

  @Test
  public void testParse() throws URISyntaxException {
    assertTrue( jdbcUrlParser.parse( "jdbc:hive2://host:80/default" ) instanceof JdbcUrlImpl );
  }
}
