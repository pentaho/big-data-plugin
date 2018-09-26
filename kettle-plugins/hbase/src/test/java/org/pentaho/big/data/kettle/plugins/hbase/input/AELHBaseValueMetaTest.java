/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.hbase.input;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.row.ValueMeta;
import org.powermock.core.classloader.annotations.PrepareForTest;

@RunWith( MockitoJUnitRunner.class )
@PrepareForTest( {ValueMeta.class} )
public class AELHBaseValueMetaTest {
  private AELHBaseValueMetaImpl stubValueMeta;

  @Before
  public void setup() throws Exception {
    stubValueMeta = new AELHBaseValueMetaImpl( true, "testAlias",
        "testColumnName", "testColumnFamily", "testMappingName",
        "testTableName" );

    stubValueMeta.setType( 5 );
    stubValueMeta.setIsLongOrDouble( false );
  }

  @Test
  public void getXmlSerializationTest() {
    StringBuilder sb = new StringBuilder(  );

    stubValueMeta.getXml( sb );

    Assert.assertTrue( sb.toString().contains( "Y" ) );
    Assert.assertTrue( sb.toString().contains( "testAlias" ) );
    Assert.assertTrue( sb.toString().contains( "testColumnName" ) );
  }

  @Test
  public void getHBaseTypeDescTest() {
    String stubType = stubValueMeta.getHBaseTypeDesc();

    Assert.assertEquals( "Integer", stubType );
  }

  @Test
  public void getHBaseTypeDescNumberTest() {
    stubValueMeta.setType( 1 );
    String stubType = stubValueMeta.getHBaseTypeDesc();

    Assert.assertEquals( "Float", stubType );
  }
}
