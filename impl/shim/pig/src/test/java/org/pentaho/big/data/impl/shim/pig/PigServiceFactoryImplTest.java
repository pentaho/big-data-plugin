/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

package org.pentaho.big.data.impl.shim.pig;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 10/1/15.
 */
public class PigServiceFactoryImplTest {

  private PigServiceFactoryImpl pigServiceFactory;
  private boolean activeConfiguration;
  private HadoopConfiguration hadoopConfiguration;
  private NamedCluster namedCluster;

  private void initialize() {
    pigServiceFactory = new PigServiceFactoryImpl( activeConfiguration, hadoopConfiguration );
  }

  @Before
  public void setup() {
    activeConfiguration = true;
    hadoopConfiguration = mock( HadoopConfiguration.class );
    namedCluster = mock( NamedCluster.class );
    initialize();
  }

  @Test
  public void testActiveCanHandle() {
    assertTrue( pigServiceFactory.canHandle( namedCluster ) );
  }

  @Test
  public void testInactiveCanHandle() {
    activeConfiguration = false;
    when( hadoopConfiguration.getIdentifier() ).thenReturn( "testId" );
    initialize();
    assertFalse( pigServiceFactory.canHandle( namedCluster ) );
  }

  @Test
  public void testCreateNoError() {
    assertTrue( pigServiceFactory.create( namedCluster ) instanceof PigServiceImpl );
  }

  @Test
  public void testCreateError() throws ConfigurationException {
    when( hadoopConfiguration.getPigShim() ).thenThrow( new ConfigurationException( "" ) );
    assertNull( pigServiceFactory.create( namedCluster ) );
  }
}
