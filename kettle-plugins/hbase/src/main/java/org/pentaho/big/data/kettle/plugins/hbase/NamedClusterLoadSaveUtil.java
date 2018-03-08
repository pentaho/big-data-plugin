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

package org.pentaho.big.data.kettle.plugins.hbase;

import org.apache.commons.lang.StringUtils;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.NamedClusterService;
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
 * 
 */
public class NamedClusterLoadSaveUtil {

  /**
   * specific for step namedclustertree
   */
  private static final String NAMED_CLUSTER_ENTRY = "NamedCluster";

  public static final String CLUSTER_NAME = "cluster_name";
  public static final String ZOOKEEPER_HOSTS = "zookeeper_hosts";
  public static final String ZOOKEEPER_PORT = "zookeeper_port";

  /**
   *  This method attempts to create named cluster based on parameters  in follow order:
   *  <ol> 
   *  <li> Load from metastore based on the name of namedCluster, if it is not possible than try next. 
   *  <li> Load from embedded in the step entry. 
   *  <li> Create a cluster template and populate only the zookeper information from step entry 
   * </ol>
   * @return named cluster which was created based on the logic above
   */
  public NamedCluster loadClusterConfig( NamedClusterService namedClusterService, ObjectId id_jobentry, Repository rep,
                                         IMetaStore metaStore, Node entrynode,
                                         LogChannelInterface logChannelInterface ) {
    // Attempt to load from metastore
    NamedCluster nc = null;
    String clusterName = null;
    try {
      if ( entrynode != null ) {
        clusterName = XMLHandler.getTagValue( entrynode, CLUSTER_NAME ); //$NON-NLS-1$
      } else if ( rep != null ) {
        clusterName = rep.getJobEntryAttributeString( id_jobentry, CLUSTER_NAME ); //$NON-NLS-1$ //$NON-NLS-2$
      }
      if ( metaStore != null && !StringUtils.isEmpty( clusterName ) && namedClusterService.contains( clusterName, metaStore ) ) {
        nc = namedClusterService.read( clusterName, metaStore );
      }
      if ( nc != null ) {
        return nc;
      }
    } catch ( Throwable t ) {
      logChannelInterface.logDebug( t.getMessage(), t );
    }
    //attemp to load from embeded in step cluster information
    if ( XMLHandler.getTagValue( entrynode, NAMED_CLUSTER_ENTRY ) != null ) {
      return namedClusterService.getClusterTemplate().fromXmlForEmbed( entrynode );
    }

    // create cluster template and fill from step ( legacy fallback )
    nc = namedClusterService.getClusterTemplate();
    if ( !StringUtils.isEmpty( clusterName ) ) {
      nc.setName( clusterName );
    }
    if ( entrynode != null ) {
      nc.setZooKeeperHost( XMLHandler.getTagValue( entrynode, ZOOKEEPER_HOSTS ) ); //$NON-NLS-1$
      nc.setZooKeeperPort( XMLHandler.getTagValue( entrynode, ZOOKEEPER_PORT ) ); //$NON-NLS-1$
    } else if ( rep != null ) {
      try {
        nc.setZooKeeperHost( rep.getJobEntryAttributeString( id_jobentry, ZOOKEEPER_HOSTS ) );
        nc.setZooKeeperPort( rep.getJobEntryAttributeString( id_jobentry, ZOOKEEPER_PORT ) ); //$NON-NLS-1$
      } catch ( KettleException ke ) {
        logChannelInterface.logError( ke.getMessage(), ke );
      }
    }
    return nc;
  }

  /**
   * The method applies to the retval follow fields with follow order
   * 
   * <ul>
   * <li> {@code <cluster_name> name of cluster </cluster_name> }
   * <li> {@code <zookeeper_hosts> host of zookeper </zookeeper_hosts> }
   * <li> {@code <zookeeper_port> port of zookeper </zookeeper_port> }
   * <li> <pre>{@code  <NamedCluster>
     <child>
        <id>  </id>
        <value> <value/>
        <type> </type>
      </child>
      ...
    </NamedCluster> }
    </pre>
   * </ul>
   */
  public void getXml( StringBuilder retval, NamedClusterService namedClusterService, NamedCluster namedCluster,
                      IMetaStore metaStore, LogChannelInterface logChannelInterface ) {
    String namedClusterName = namedCluster.getName();
    String m_zookeeperHosts = namedCluster.getZooKeeperHost();
    String m_zookeeperPort = namedCluster.getZooKeeperPort();

    if ( !StringUtils.isEmpty( namedClusterName ) ) {
      retval.append( "\n    " ).append( XMLHandler.addTagValue( CLUSTER_NAME, namedClusterName ) ); //$NON-NLS-1$ //$NON-NLS-2$
      try {
        if ( metaStore != null && namedClusterService.contains( namedClusterName, metaStore ) ) {
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
    //add full named cluster to use when metastore does not available
    retval.append(  namedCluster.toXmlForEmbed( NAMED_CLUSTER_ENTRY ) );
  }

  /**
   * The method save to the rep follow fields with follow order
   * <ul>
   * <li> {@code <cluster_name> name of cluster </cluster_name> }
   * <li> {@code <zookeeper_hosts> host of zookeper </zookeeper_hosts> }
   * <li> {@code <zookeeper_port> port of zookeper </zookeeper_port> }
   * <li> <pre>{@code  <NamedCluster>
     <child>
        <id>  </id>
        <value> <value/>
        <type> </type>
      </child>
      ...
    </NamedCluster> }
    </pre>
   * </ul>
   */
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
    rep.saveStepAttribute( id_transformation, id_step, NAMED_CLUSTER_ENTRY, namedCluster.toXmlForEmbed( NAMED_CLUSTER_ENTRY ) );
  }
}
