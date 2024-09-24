/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.mapreduce.step.enter;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.injection.BaseMetadataInjectionTest;

public class HadoopEnterMetaInjectionTest extends BaseMetadataInjectionTest<HadoopEnterMeta> {
  @Before
  public void setup() throws Throwable {
    setup( new HadoopEnterMeta() );
  }

  @Test
  public void test() throws Exception {
    check( "KEY_TYPE", new IntGetter() {
      public int get() {
        return meta.getType()[0];
      }
    } );
    check( "KEY_LENGTH", new IntGetter() {
      public int get() {
        return meta.getLength()[0];
      }
    } );
    check( "KEY_PRECISION", new IntGetter() {
      public int get() {
        return meta.getPrecision()[0];
      }
    } );
    check( "VALUE_TYPE", new IntGetter() {
      public int get() {
        return meta.getType()[1];
      }
    } );
    check( "VALUE_LENGTH", new IntGetter() {
      public int get() {
        return meta.getLength()[1];
      }
    } );
    check( "VALUE_PRECISION", new IntGetter() {
      public int get() {
        return meta.getPrecision()[1];
      }
    } );
  }
}
