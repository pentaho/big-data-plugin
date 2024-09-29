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
