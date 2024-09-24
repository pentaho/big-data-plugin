/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
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
