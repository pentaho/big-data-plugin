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
import org.pentaho.hbase.shim.spi.HBaseConnection;

/**
 * Created by bryan on 2/2/16.
 */
public interface HBaseConnectionTestImpls {
  abstract class HBaseConnectionWithResultField extends HBaseConnection {
    private Result m_currentResultSetRow;

    public static abstract class Subclass extends HBaseConnectionWithResultField {
    }
  }

  abstract class HBaseConnectionWithMismatchedDelegate extends HBaseConnection {
    private Object delegate;
  }

  abstract class HBaseConnectionWithPublicDelegate extends HBaseConnection {
    public Object delegate;
  }
}
