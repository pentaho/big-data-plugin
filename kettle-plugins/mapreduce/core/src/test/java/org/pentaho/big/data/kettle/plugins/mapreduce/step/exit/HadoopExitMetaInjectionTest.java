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

package org.pentaho.big.data.kettle.plugins.mapreduce.step.exit;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.injection.BaseMetadataInjectionTest;

public class HadoopExitMetaInjectionTest extends BaseMetadataInjectionTest<HadoopExitMeta> {
  @Before
  public void setup() throws Throwable {
    setup( new HadoopExitMeta() );
  }

  @Test
  public void test() throws Exception {
    check( "KEY_FIELD", new StringGetter() {
      public String get() {
        return meta.getOutKeyFieldname();
      }
    } );
    check( "VALUE_FIELD", new StringGetter() {
      public String get() {
        return meta.getOutValueFieldname();
      }
    } );
  }
}
