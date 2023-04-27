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


package org.pentaho.big.data.kettle.plugins.mapreduce;

import org.junit.Test;
import org.pentaho.big.data.kettle.plugins.mapreduce.entry.hadoop.JobEntryHadoopJobExecutor;
import org.pentaho.big.data.kettle.plugins.mapreduce.entry.pmr.JobEntryHadoopTransJobExecutor;
import org.pentaho.big.data.kettle.plugins.mapreduce.step.enter.HadoopEnterMeta;
import org.pentaho.big.data.kettle.plugins.mapreduce.step.exit.HadoopExitMeta;
import org.pentaho.big.data.kettle.plugins.mapreduce.ui.entry.hadoop.JobEntryHadoopJobExecutorDialog;
import org.pentaho.big.data.kettle.plugins.mapreduce.ui.entry.pmr.JobEntryHadoopTransJobExecutorDialog;
import org.pentaho.big.data.kettle.plugins.mapreduce.ui.step.enter.HadoopEnterDialog;
import org.pentaho.big.data.kettle.plugins.mapreduce.ui.step.exit.HadoopExitDialog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by bryan on 1/15/16.
 */
public class DialogClassUtilTest {
  @Test
  public void testConstructor() {
    assertNotNull( new DialogClassUtil() );
  }

  @Test
  public void testJobEntryHadoopTransJobExecutor() {
    assertEquals( JobEntryHadoopTransJobExecutorDialog.class.getCanonicalName(), DialogClassUtil.getDialogClassName(
      JobEntryHadoopTransJobExecutor.class ) );
  }

  @Test
  public void testJobEntryHadoopJobExecutor() {
    assertEquals( JobEntryHadoopJobExecutorDialog.class.getCanonicalName(), DialogClassUtil.getDialogClassName(
      JobEntryHadoopJobExecutor.class ) );
  }

  @Test
  public void testHadoopExit() {
    assertEquals( HadoopExitDialog.class.getCanonicalName(), DialogClassUtil.getDialogClassName(
      HadoopExitMeta.class ) );
  }

  @Test
  public void testHadoopEnter() {
    assertEquals( HadoopEnterDialog.class.getCanonicalName(), DialogClassUtil.getDialogClassName(
      HadoopEnterMeta.class ) );
  }
}
