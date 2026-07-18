/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/



package org.pentaho.amazon.emr.job;

import java.util.Arrays;
import java.util.List;

import org.pentaho.di.job.entry.loadSave.JobEntryLoadSaveTestSupport;

public class AmazonElasticMapReduceJobExecutorLoadSaveTest
extends JobEntryLoadSaveTestSupport<AmazonElasticMapReduceJobExecutor> {

  @Override
  protected Class<AmazonElasticMapReduceJobExecutor> getJobEntryClass() {
    return AmazonElasticMapReduceJobExecutor.class;
  }

  @Override
  protected List<String> listCommonAttributes() {
    return Arrays.asList( "HadoopJobName", "HadoopJobFlowId", "JarUrl", "AccessKey", "SecretKey",
      "StagingDir", "NumInstances", "MasterInstanceType", "SlaveInstanceType", "CmdLineArgs",
      "Blocking", "LoggingInterval", "HadoopJobName" );
  }

}
