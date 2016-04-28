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

package com.pentaho.big.data.bundles.impl.shim.hive;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.big.data.api.jdbc.JdbcUrlParser;

import java.sql.Driver;

import static org.junit.Assert.assertTrue;

/**
 * Created by bryan on 4/18/16.
 */
@RunWith( MockitoJUnitRunner.class )
public class ImpalaDriverTest {
  @Mock Driver delegate;
  @Mock JdbcUrlParser jdbcUrlParser;
  private ImpalaDriver impalaDriver;

  @Before
  public void setup() {
    impalaDriver = new ImpalaDriver( delegate, null, true, jdbcUrlParser );
  }

  @Test
  public void testConstructor() {
    assertTrue( impalaDriver instanceof ImpalaDriver );
  }
}
