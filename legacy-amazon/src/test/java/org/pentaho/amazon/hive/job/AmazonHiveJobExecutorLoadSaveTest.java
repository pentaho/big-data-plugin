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

package org.pentaho.amazon.hive.job;

import java.util.Arrays;
import java.util.List;

import org.pentaho.di.job.entry.loadSave.JobEntryLoadSaveTestSupport;

public class AmazonHiveJobExecutorLoadSaveTest extends JobEntryLoadSaveTestSupport<AmazonHiveJobExecutor> {

  @Override
  protected Class<AmazonHiveJobExecutor> getJobEntryClass() {
    return AmazonHiveJobExecutor.class;
  }

  @Override
  protected List<String> listCommonAttributes() {
    return Arrays.asList( "HadoopJobName", "HadoopJobFlowId", "QUrl", "AccessKey", "SecretKey",
      "BootstrapActions", "StagingDir", "NumInstances", "MasterInstanceType", "SlaveInstanceType",
      "CmdLineArgs", "Alive", "Blocking", "LoggingInterval", "HadoopJobName" );
  }
}
