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

package com.pentaho.big.data.bundles.impl.shim.hbase;

import org.apache.hadoop.hbase.client.Result;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.bigdata.api.hbase.ResultFactoryException;
import org.pentaho.hbase.shim.spi.HBaseBytesUtilShim;

import java.nio.charset.Charset;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 2/2/16.
 */
public class ResultFactoryImplTest {

  private HBaseBytesUtilShim hBaseBytesUtilShim;
  private ResultFactoryImpl resultFactory;

  @Before
  public void setup() {
    hBaseBytesUtilShim = mock( HBaseBytesUtilShim.class );
    resultFactory = new ResultFactoryImpl( hBaseBytesUtilShim );
  }

  @Test
  public void testCanHandle() {
    assertTrue( resultFactory.canHandle( null ) );
    assertTrue( resultFactory.canHandle( mock( Result.class ) ) );
    assertFalse( resultFactory.canHandle( new Object() ) );
  }

  @Test
  public void testCreateNull() throws ResultFactoryException {
    assertNull( resultFactory.create( null ) );
  }

  @Test
  public void testCreateSuccess() throws ResultFactoryException {
    Result delegate = mock( Result.class );
    byte[] row = "row".getBytes( Charset.forName( "UTF-8" ) );
    when( delegate.getRow() ).thenReturn( row );

    ResultImpl result = resultFactory.create( delegate );
    assertArrayEquals( row, result.getRow() );
  }

  @Test( expected = ResultFactoryException.class )
  public void testCreateException() throws ResultFactoryException {
    resultFactory.create( new Object() );
  }
}
