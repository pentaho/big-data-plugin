/*******************************************************************************
 *
 * Pentaho Big Data
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
