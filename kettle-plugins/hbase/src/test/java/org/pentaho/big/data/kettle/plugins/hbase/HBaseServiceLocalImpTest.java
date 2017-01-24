/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
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
package org.pentaho.big.data.kettle.plugins.hbase;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;

public class HBaseServiceLocalImpTest {
  private HBaseServiceLocalImp hBaseLocalService;

  @Before
  public void setup() {
    hBaseLocalService = new HBaseServiceLocalImp();
  }

  @Test
  public void testGetHBaseConnection() throws IOException {
    assertNull( hBaseLocalService.getHBaseConnection( mock( VariableSpace.class ), "siteConfig", "defConfig", mock( LogChannelInterface.class ) ) );
  }

  @Test
  public void testGetColumnFilterFactory() {
    assertNotNull( hBaseLocalService.getColumnFilterFactory() );
  }

  @Test
  public void testGetMappingFactory() {
    assertNotNull( hBaseLocalService.getMappingFactory() );
  }

  @Test
  public void testGetHBaseValueMetaInterfaceFactory() {
    assertNotNull( hBaseLocalService.getHBaseValueMetaInterfaceFactory() );
  }

  @Test
  public void testGetByteConversionUtil() {
    assertNotNull( hBaseLocalService.getByteConversionUtil() );
  }

  @Test
  public void testGetResultFactory() {
    assertNull( hBaseLocalService.getResultFactory() );
  }

}
