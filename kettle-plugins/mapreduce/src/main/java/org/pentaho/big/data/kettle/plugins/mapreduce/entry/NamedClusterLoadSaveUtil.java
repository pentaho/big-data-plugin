/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.mapreduce.entry;

import org.apache.commons.lang.StringUtils;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.w3c.dom.Node;

/**
 * Created by bryan on 1/6/16.
 */
public class NamedClusterLoadSaveUtil {
  public static final String CLUSTER_NAME = "cluster_name";
  public static final String HDFS_HOSTNAME = "hdfs_hostname";
  public static final String HDFS_PORT = "hdfs_port";
  public static final String JOB_TRACKER_HOSTNAME = "job_tracker_hostname";
  public static final String JOB_TRACKER_PORT = "job_tracker_port";

  public void saveNamedClusterRep( NamedCluster namedCluster, NamedClusterService namedClusterService, Repository rep,
      IMetaStore metaStore, ObjectId id_job, ObjectId objectId, LogChannelInterface logChannelInterface )
        throws KettleException {
    if ( namedCluster != null ) {
      String namedClusterName = namedCluster.getName();
      if ( !Const.isEmpty( namedClusterName ) ) {
        rep.saveJobEntryAttribute( id_job, objectId, CLUSTER_NAME, namedClusterName ); // $NON-NLS-1$
      }
      try {
        if ( !StringUtils.isEmpty( namedClusterName ) && namedClusterService.contains( namedClusterName, metaStore ) ) {
          // pull config from NamedCluster
          namedCluster = namedClusterService.read( namedClusterName, metaStore );
        }
      } catch ( MetaStoreException e ) {
        logChannelInterface.logDebug( e.getMessage(), e );
      }
      rep.saveJobEntryAttribute( id_job, objectId, HDFS_HOSTNAME, namedCluster.getHdfsHost() ); // $NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, objectId, HDFS_PORT, namedCluster.getHdfsPort() ); // $NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, objectId, JOB_TRACKER_HOSTNAME, namedCluster.getJobTrackerHost() ); // $NON-NLS-1$
      rep.saveJobEntryAttribute( id_job, objectId, JOB_TRACKER_PORT, namedCluster.getJobTrackerPort() ); // $NON-NLS-1$
    }
  }

  public void getXmlNamedCluster( NamedCluster namedCluster, NamedClusterService namedClusterService, IMetaStore metaStore,
      LogChannelInterface logChannelInterface, StringBuilder retval ) {
    if ( namedCluster != null ) {
      String namedClusterName = namedCluster.getName();
      if ( !Const.isEmpty( namedClusterName ) ) {
        retval.append( "      " ).append( XMLHandler.addTagValue( CLUSTER_NAME, namedClusterName ) ); // $NON-NLS-1$
                                                                                                      // //$NON-NLS-2$
      }
      try {
        if ( metaStore != null && !StringUtils.isEmpty( namedClusterName ) && namedClusterService
          .contains( namedClusterName, metaStore ) ) {
          // pull config from NamedCluster
          namedCluster = namedClusterService.read( namedClusterName, metaStore );
        }
      } catch ( MetaStoreException e ) {
        logChannelInterface.logDebug( e.getMessage(), e );
      }
      retval.append( "      " ).append( XMLHandler.addTagValue( HDFS_HOSTNAME, namedCluster.getHdfsHost() ) ); // $NON-NLS-1$
                                                                                                               // //$NON-NLS-2$
      retval.append( "      " ).append( XMLHandler.addTagValue( HDFS_PORT, namedCluster.getHdfsPort() ) ); // $NON-NLS-1$
                                                                                                           // //$NON-NLS-2$
      retval.append( "      " ).append( XMLHandler.addTagValue( JOB_TRACKER_HOSTNAME, namedCluster
          .getJobTrackerHost() ) ); // $NON-NLS-1$ //$NON-NLS-2$
      retval.append( "      " ).append( XMLHandler.addTagValue( JOB_TRACKER_PORT, namedCluster.getJobTrackerPort() ) ); // $NON-NLS-1$
                                                                                                                        // //$NON-NLS-2$
    }
  }

  public NamedCluster loadClusterConfig( NamedClusterService namedClusterService, ObjectId id_jobentry, Repository rep,
                                         IMetaStore metaStore, Node entrynode,
                                         LogChannelInterface logChannelInterface ) {
    boolean configLoaded = false;
    try {
      String clusterName = null;
      // attempt to load from named cluster
      if ( entrynode != null ) {
        clusterName = XMLHandler.getTagValue( entrynode, CLUSTER_NAME ); //$NON-NLS-1$
      } else if ( rep != null ) {
        clusterName = rep.getJobEntryAttributeString( id_jobentry, CLUSTER_NAME ); //$NON-NLS-1$ //$NON-NLS-2$
      }
      // load from system first, then fall back to copy stored with job (AbstractMeta)
      NamedCluster nc = null;
      if ( metaStore != null && !StringUtils.isEmpty( clusterName )
        && namedClusterService.contains( clusterName, metaStore ) ) {
        // pull config from NamedCluster
        nc = namedClusterService.read( clusterName, metaStore );
      }
      if ( nc != null ) {
        return nc;
      }
    } catch ( Throwable t ) {
      logChannelInterface.logDebug( t.getMessage(), t );
    }

    NamedCluster namedCluster = namedClusterService.getClusterTemplate();
    if ( entrynode != null ) {
      // load default values for cluster & legacy fallback
      namedCluster.setHdfsHost( XMLHandler.getTagValue( entrynode, HDFS_HOSTNAME ) ); //$NON-NLS-1$
      namedCluster.setHdfsPort( XMLHandler.getTagValue( entrynode, HDFS_PORT ) ); //$NON-NLS-1$
      namedCluster.setJobTrackerHost( XMLHandler.getTagValue( entrynode, JOB_TRACKER_HOSTNAME ) ); //$NON-NLS-1$
      namedCluster.setJobTrackerPort( XMLHandler.getTagValue( entrynode, JOB_TRACKER_PORT ) ); //$NON-NLS-1$
    } else if ( rep != null ) {
      // load default values for cluster & legacy fallback
      try {
        namedCluster.setHdfsHost( rep.getJobEntryAttributeString( id_jobentry, HDFS_HOSTNAME ) );
        namedCluster.setHdfsPort( rep.getJobEntryAttributeString( id_jobentry, HDFS_PORT ) ); //$NON-NLS-1$
        namedCluster
          .setJobTrackerHost( rep.getJobEntryAttributeString( id_jobentry, JOB_TRACKER_HOSTNAME ) ); //$NON-NLS-1$
        namedCluster
          .setJobTrackerPort( rep.getJobEntryAttributeString( id_jobentry, JOB_TRACKER_PORT ) ); //$NON-NLS-1$
      } catch ( KettleException ke ) {
        logChannelInterface.logError( ke.getMessage(), ke );
      }
    }
    return namedCluster;
  }
}
