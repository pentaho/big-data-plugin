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

package org.pentaho.big.data.kettle.plugins.hbase;

import org.apache.commons.lang.StringUtils;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.w3c.dom.Node;

/**
 * Created by bryan on 1/19/16.
 */
public class NamedClusterLoadSaveUtil {
  public static final String CLUSTER_NAME = "cluster_name";
  public static final String ZOOKEEPER_HOSTS = "zookeeper_hosts";
  public static final String ZOOKEEPER_PORT = "zookeeper_port";

  public NamedCluster loadClusterConfig( NamedClusterService namedClusterService, ObjectId id_jobentry, Repository rep,
                                         IMetaStore metaStore, Node entrynode,
                                         LogChannelInterface logChannelInterface ) {
    // load from system first, then fall back to copy stored with job (AbstractMeta)
    NamedCluster nc = null;
    String clusterName = null;
    try {
      // attempt to load from named cluster
      if ( entrynode != null ) {
        clusterName = XMLHandler.getTagValue( entrynode, CLUSTER_NAME ); //$NON-NLS-1$
      } else if ( rep != null ) {
        clusterName = rep.getJobEntryAttributeString( id_jobentry, CLUSTER_NAME ); //$NON-NLS-1$ //$NON-NLS-2$
      }

      if ( !StringUtils.isEmpty( clusterName ) ) {
        nc = namedClusterService.getNamedClusterByName( clusterName, metaStore );
      }

      if ( nc != null ) {
        return nc;
      }
    } catch ( Throwable t ) {
      logChannelInterface.logDebug( t.getMessage(), t );
    }

    nc = namedClusterService.getClusterTemplate();
    if ( !StringUtils.isEmpty( clusterName ) ) {
      nc.setName( clusterName );
    }
    if ( entrynode != null ) {
      // load default values for cluster & legacy fallback
      nc.setZooKeeperHost( XMLHandler.getTagValue( entrynode, ZOOKEEPER_HOSTS ) ); //$NON-NLS-1$
      nc.setZooKeeperPort( XMLHandler.getTagValue( entrynode, ZOOKEEPER_PORT ) ); //$NON-NLS-1$
    } else if ( rep != null ) {
      // load default values for cluster & legacy fallback
      try {
        nc.setZooKeeperHost( rep.getJobEntryAttributeString( id_jobentry, ZOOKEEPER_HOSTS ) );
        nc.setZooKeeperPort( rep.getJobEntryAttributeString( id_jobentry, ZOOKEEPER_PORT ) ); //$NON-NLS-1$
      } catch ( KettleException ke ) {
        logChannelInterface.logError( ke.getMessage(), ke );
      }
    }
    return nc;
  }

  public void getXml( StringBuilder retval, NamedClusterService namedClusterService, NamedCluster namedCluster,
                      IMetaStore metaStore, LogChannelInterface logChannelInterface ) {
    String namedClusterName = namedCluster.getName();
    String m_zookeeperHosts = namedCluster.getZooKeeperHost();
    String m_zookeeperPort = namedCluster.getZooKeeperPort();

    if ( !StringUtils.isEmpty( namedClusterName ) ) {
      retval.append( "\n    " )
        .append( XMLHandler.addTagValue( CLUSTER_NAME, namedClusterName ) ); //$NON-NLS-1$ //$NON-NLS-2$
      try {
        if ( metaStore != null && namedClusterService.contains( namedClusterName, metaStore ) ) {
          // pull config from NamedCluster
          NamedCluster nc = namedClusterService.read( namedClusterName, metaStore );
          if ( nc != null ) {
            m_zookeeperHosts = nc.getZooKeeperHost();
            m_zookeeperPort = nc.getZooKeeperPort();
          }
        }
      } catch ( MetaStoreException e ) {
        logChannelInterface.logDebug( e.getMessage(), e );
      }
    }

    if ( !Utils.isEmpty( m_zookeeperHosts ) ) {
      retval.append( "\n    " ).append( XMLHandler.addTagValue( ZOOKEEPER_HOSTS, m_zookeeperHosts ) );
    }
    if ( !Utils.isEmpty( m_zookeeperPort ) ) {
      retval.append( "\n    " ).append( XMLHandler.addTagValue( ZOOKEEPER_PORT, m_zookeeperPort ) );
    }
  }

  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step,
                       NamedClusterService namedClusterService, NamedCluster namedCluster,
                       LogChannelInterface logChannelInterface )
    throws KettleException {
    String namedClusterName = namedCluster.getName();
    String m_zookeeperHosts = namedCluster.getZooKeeperHost();
    String m_zookeeperPort = namedCluster.getZooKeeperPort();

    if ( !StringUtils.isEmpty( namedClusterName ) ) {
      rep.saveStepAttribute( id_transformation, id_step, CLUSTER_NAME, namedClusterName ); //$NON-NLS-1$
      try {
        if ( namedClusterService.contains( namedClusterName, metaStore ) ) {
          // pull config from NamedCluster
          NamedCluster nc = namedClusterService.read( namedClusterName, metaStore );
          m_zookeeperHosts = nc.getZooKeeperHost();
          m_zookeeperPort = nc.getZooKeeperPort();
        }
      } catch ( MetaStoreException e ) {
        logChannelInterface.logDebug( e.getMessage(), e );
      }
    }

    if ( !Utils.isEmpty( m_zookeeperHosts ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, ZOOKEEPER_HOSTS, m_zookeeperHosts );
    }
    if ( !Utils.isEmpty( m_zookeeperPort ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, ZOOKEEPER_PORT, m_zookeeperPort );
    }
  }
}
