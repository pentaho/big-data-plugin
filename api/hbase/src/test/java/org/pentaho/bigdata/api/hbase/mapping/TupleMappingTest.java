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

package org.pentaho.bigdata.api.hbase.mapping;

import org.junit.Test;
import org.pentaho.bigdata.api.hbase.mapping.Mapping;

import static org.junit.Assert.assertEquals;

/**
 * Created by bryan on 2/2/16.
 */
public class TupleMappingTest {
  @Test
  public void testKey() {
    assertEquals( "KEY", Mapping.TupleMapping.KEY.toString() );
  }

  @Test
  public void testFamily() {
    assertEquals( "Family", Mapping.TupleMapping.FAMILY.toString() );
  }

  @Test
  public void testColumn() {
    assertEquals( "Column", Mapping.TupleMapping.COLUMN.toString() );
  }

  @Test
  public void testValue() {
    assertEquals( "Value", Mapping.TupleMapping.VALUE.toString() );
  }

  @Test
  public void testTimestamp() {
    assertEquals( "Timestamp", Mapping.TupleMapping.TIMESTAMP.toString() );
  }

  @Test
  public void testValueOfAndValues() {
    for ( Mapping.TupleMapping tupleMapping : Mapping.TupleMapping.values() ) {
      assertEquals( tupleMapping, Mapping.TupleMapping.valueOf( tupleMapping.name() ) );
    }
  }
}
