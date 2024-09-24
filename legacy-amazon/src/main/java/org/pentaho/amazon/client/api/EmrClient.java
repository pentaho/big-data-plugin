/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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
