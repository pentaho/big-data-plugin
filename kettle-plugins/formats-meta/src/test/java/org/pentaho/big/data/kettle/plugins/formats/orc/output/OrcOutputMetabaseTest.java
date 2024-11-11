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

package org.pentaho.big.data.kettle.plugins.formats.orc.output;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;

import static org.mockito.Mockito.spy;

/**
 * Created by rmansoor on 4/8/2018.
 */
@RunWith( MockitoJUnitRunner.class )
public class OrcOutputMetabaseTest {
  private OrcOutputMetaBase metaBase;

  @Before
  public void setUp() throws Exception {
    metaBase = spy( new OrcOutputMetaBase() {
      @Override
      public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
          TransMeta transMeta,
          Trans trans ) {
        return null;
      }

      @Override public StepDataInterface getStepData() {
        return null;
      }
    } );
  }

  @Test
  public void setCompressionType() {
    metaBase.setCompressionType( "snappy" );
    Assert.assertTrue( metaBase.getCompressionType().equals( OrcOutputMetaBase.CompressionType.SNAPPY.toString() ) );
    metaBase.setCompressionType( "Snappy" );
    Assert.assertTrue( metaBase.getCompressionType().equals( OrcOutputMetaBase.CompressionType.SNAPPY.toString() ) );
    metaBase.setCompressionType( "SNAPPY" );
    Assert.assertTrue( metaBase.getCompressionType().equals( OrcOutputMetaBase.CompressionType.SNAPPY.toString() ) );
    metaBase.setCompressionType( "zlib" );
    Assert.assertTrue( metaBase.getCompressionType().equals( OrcOutputMetaBase.CompressionType.ZLIB.toString() ) );
    metaBase.setCompressionType( "Zlib" );
    Assert.assertTrue( metaBase.getCompressionType().equals( OrcOutputMetaBase.CompressionType.ZLIB.toString() ) );
    metaBase.setCompressionType( "ZLIB" );
    Assert.assertTrue( metaBase.getCompressionType().equals( OrcOutputMetaBase.CompressionType.ZLIB.toString() ) );
    metaBase.setCompressionType( "lzo" );
    Assert.assertTrue( metaBase.getCompressionType().equals( OrcOutputMetaBase.CompressionType.LZO.toString() ) );
    metaBase.setCompressionType( "Lzo" );
    Assert.assertTrue( metaBase.getCompressionType().equals( OrcOutputMetaBase.CompressionType.LZO.toString() ) );
    metaBase.setCompressionType( "LZO" );
    Assert.assertTrue( metaBase.getCompressionType().equals( OrcOutputMetaBase.CompressionType.LZO.toString() ) );
    metaBase.setCompressionType( "None" );
    Assert.assertTrue( metaBase.getCompressionType().equals( OrcOutputMetaBase.CompressionType.NONE.toString() ) );
    metaBase.setCompressionType( "none" );
    Assert.assertTrue( metaBase.getCompressionType().equals( OrcOutputMetaBase.CompressionType.NONE.toString() ) );
    metaBase.setCompressionType( "NONE" );
    Assert.assertTrue( metaBase.getCompressionType().equals( OrcOutputMetaBase.CompressionType.NONE.toString() ) );
  }
}
