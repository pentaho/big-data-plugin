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
