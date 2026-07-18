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



package org.pentaho.amazon.client.api;

import java.io.File;

/**
 * Created by Aliaksandr_Zhuk on 2/5/2018.
 */
public interface S3Client {

  /**
   * Copied from
   * @class S3FileProvider.java
   * @module s3-vfs
   */
  String SCHEME = "s3";

  void createBucketIfNotExists( String stagingBucketName );

  void deleteObjectFromBucket( String stagingBucketName, String key );

  void putObjectInBucket( String stagingBucketName, String key, File tmpFile );

  String readStepLogsFromS3( String stagingBucketName, String hadoopJobFlowId, String stepId );
}
