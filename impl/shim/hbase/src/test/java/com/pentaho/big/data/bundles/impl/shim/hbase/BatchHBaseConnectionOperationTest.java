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

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;

import static org.mockito.Mockito.*;

/**
 * Created by bryan on 2/2/16.
 */
public class BatchHBaseConnectionOperationTest {
  @Test
  public void testBatchHBaseConnectionOperationTest() throws IOException {
    final HBaseConnectionWrapper hBaseConnectionWrapper = mock( HBaseConnectionWrapper.class );
    BatchHBaseConnectionOperation batchHBaseConnectionOperation = new BatchHBaseConnectionOperation();
    HBaseConnectionOperation operation1 = mock( HBaseConnectionOperation.class );
    final HBaseConnectionOperation operation2 = mock( HBaseConnectionOperation.class );
    batchHBaseConnectionOperation.addOperation( operation1 );
    batchHBaseConnectionOperation.addOperation( operation2 );

    doAnswer( new Answer<Void>() {
      @Override public Void answer( InvocationOnMock invocation ) throws Throwable {
        verify( operation2, never() ).perform( hBaseConnectionWrapper );
        return null;
      }
    } ).when( operation1 ).perform( hBaseConnectionWrapper );

    batchHBaseConnectionOperation.perform( hBaseConnectionWrapper );

    verify( operation1 ).perform( hBaseConnectionWrapper );
    verify( operation2 ).perform( hBaseConnectionWrapper );
  }
}
