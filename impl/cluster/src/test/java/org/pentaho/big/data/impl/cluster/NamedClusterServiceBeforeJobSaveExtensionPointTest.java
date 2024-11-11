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

package org.pentaho.big.data.impl.cluster;

import org.junit.Test;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.osgi.api.NamedClusterServiceOsgi;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobExecutionExtension;
import org.pentaho.di.job.JobMeta;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by tkafalas on 8/11/2017.
 */
public class NamedClusterServiceBeforeJobSaveExtensionPointTest {

  @Test
  public void testCallExtensionPoint() throws Exception {
    NamedClusterService mockNamedClusterService = mock( NamedClusterService.class );
    LogChannelInterface logChannelInterface = mock( LogChannelInterface.class );
    JobExecutionExtension mockJobExecutionExtension = mock( JobExecutionExtension.class );
    Job mockJob = mock( Job.class );
    JobMeta mockJobMeta = mock( JobMeta.class );
    mockJobExecutionExtension.job = mockJob;
    when( mockJob.getJobMeta() ).thenReturn( mockJobMeta );

    NamedClusterServiceBeforeJobSaveExtensionPoint namedClusterServiceExtensionPoint =
      new NamedClusterServiceBeforeJobSaveExtensionPoint( mockNamedClusterService );

    namedClusterServiceExtensionPoint.callExtensionPoint( logChannelInterface, mockJobExecutionExtension );
    verify( mockJobMeta ).setNamedClusterServiceOsgi( any( NamedClusterServiceOsgi.class ) );
  }

}
