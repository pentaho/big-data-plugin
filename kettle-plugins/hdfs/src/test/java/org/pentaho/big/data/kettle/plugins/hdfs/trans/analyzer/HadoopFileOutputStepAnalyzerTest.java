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
import org.pentaho.big.data.kettle.plugins.hdfs.trans.HadoopFileOutputMeta;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith( MockitoJUnitRunner.class )
public class HadoopFileOutputStepAnalyzerTest extends HadoopBaseStepAnalyzerTest<HadoopFileOutputStepAnalyzer,
  HadoopFileOutputMeta> {

  @Mock private HadoopFileOutputMeta meta;

  @Override
  protected HadoopFileOutputStepAnalyzer getAnalyzer() {
    return new HadoopFileOutputStepAnalyzer();
  }

  @Override
  protected HadoopFileOutputMeta getMetaMock() {
    return meta;
  }

  @Override
  protected Class<HadoopFileOutputMeta> getMetaClass() {
    return HadoopFileOutputMeta.class;
  }

  @Test
  public void testIsOutput() {
    assertTrue( analyzer.isOutput() );
  }

  @Test
  public void testIsInput() {
    assertFalse( analyzer.isInput() );
  }
}
