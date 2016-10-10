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
package com.pentaho.big.data.bundles.impl.shim.hbase.meta;



import org.junit.Before;
import org.junit.Test;
import org.pentaho.hbase.shim.api.HBaseValueMeta;
import org.pentaho.hbase.shim.spi.HBaseBytesUtilShim;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.junit.Assert.assertEquals;

public class HBaseValueMetaInterfaceFactoryImplTest {
  HBaseValueMetaInterfaceFactoryImpl hBaseValueMetaInterfaceFactory;
  @Before
  public void setUp() throws Exception {
    HBaseBytesUtilShim hBaseBytesUtilShim = mock( HBaseBytesUtilShim.class );
    hBaseValueMetaInterfaceFactory = new HBaseValueMetaInterfaceFactoryImpl( hBaseBytesUtilShim );
  }
  @Test
  public void testCopyIsLongOrDoubleType() throws Exception {
    HBaseValueMeta hBaseLongValueMeta = new HBaseValueMeta( "col_family_1,col_name_1,long", 5, 0, 0 );
    hBaseLongValueMeta.setIsLongOrDouble( true );
    HBaseValueMeta hBaseIntegerValueMeta = new HBaseValueMeta( "col_family_2,col_name_2,integer", 5, 0, 0 );
    hBaseIntegerValueMeta.setIsLongOrDouble( false );
    HBaseValueMeta hBaseDoubleValueMeta = new HBaseValueMeta( "col_family_3,col_name_3,double", 2, 0, 0 );
    hBaseDoubleValueMeta.setIsLongOrDouble( true );
    HBaseValueMeta hBaseFloatValueMeta = new HBaseValueMeta( "col_family_4,col_name_4,float", 2, 0, 0 );
    hBaseFloatValueMeta.setIsLongOrDouble( false );

    List<HBaseValueMeta> metaToCopyList = new ArrayList<>();
    metaToCopyList.add( hBaseLongValueMeta );
    metaToCopyList.add( hBaseLongValueMeta );
    metaToCopyList.add( hBaseDoubleValueMeta );
    metaToCopyList.add( hBaseFloatValueMeta );

    HBaseValueMetaInterfaceImpl metaAfterCopy;
    for ( HBaseValueMeta metaToCopy : metaToCopyList ) {
      metaAfterCopy = hBaseValueMetaInterfaceFactory.copy( metaToCopy );
      assertEquals( metaToCopy.getType(), metaAfterCopy.getType() );
      assertEquals( metaToCopy.getIsLongOrDouble(), metaToCopy.getIsLongOrDouble() );
      assertEquals( metaToCopy.getHBaseTypeDesc(), metaAfterCopy.getHBaseTypeDesc() );
    }
  }


}
