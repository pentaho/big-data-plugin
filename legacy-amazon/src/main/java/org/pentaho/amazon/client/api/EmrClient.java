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

package org.pentaho.amazon.client.api;

import org.pentaho.amazon.AbstractAmazonJobEntry;

import java.net.URISyntaxException;

/**
 * Created by Aliaksandr_Zhuk on 2/5/2018.
 */
public interface EmrClient {

  void runJobFlow( String stagingS3FileUrl, String stagingS3BucketUrl, String stepType, String mainClass,
                   String bootstrapActions,
                   AbstractAmazonJobEntry jobEntry
  );

  String getHadoopJobFlowId();

  String getStepId();

  void addStepToExistingJobFlow( String stagingS3FileUrl, String stagingS3BucketUrl, String stepType, String mainClass,
                                 AbstractAmazonJobEntry jobEntry );

  String getCurrentClusterState();

  String getCurrentStepState();

  boolean isClusterRunning();

  boolean isStepRunning();

  boolean isRunning();

  boolean isClusterTerminated();

  boolean isStepFailed();

  boolean isStepNotSuccess();

  String getJobFlowLogUri()throws URISyntaxException;

  boolean stopSteps();
}
