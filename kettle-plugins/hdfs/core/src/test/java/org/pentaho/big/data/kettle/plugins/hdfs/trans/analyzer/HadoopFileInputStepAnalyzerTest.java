/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.big.data.kettle.plugins.hdfs.trans.analyzer;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.big.data.kettle.plugins.hdfs.trans.HadoopFileInputMeta;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith( MockitoJUnitRunner.class )
public class HadoopFileInputStepAnalyzerTest
  extends HadoopBaseStepAnalyzerTest<HadoopFileInputStepAnalyzer, HadoopFileInputMeta> {

  @Mock private HadoopFileInputMeta meta;

  @Override
  protected HadoopFileInputStepAnalyzer getAnalyzer() {
    return new HadoopFileInputStepAnalyzer();
  }

  @Override
  protected HadoopFileInputMeta getMetaMock() {
    return meta;
  }

  @Override
  protected Class<HadoopFileInputMeta> getMetaClass() {
    return HadoopFileInputMeta.class;
  }

  @Test
  public void testIsOutput() {
    assertFalse( analyzer.isOutput() );
  }

  @Test
  public void testIsInput() {
    assertTrue( analyzer.isInput() );
  }
}
