/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.core.namedcluster;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.vfs.FileName;
import org.apache.commons.vfs.provider.url.UrlFileName;
import org.apache.commons.vfs.provider.url.UrlFileNameParser;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.hadoop.HadoopSpoonPlugin;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;

public class NamedClusterManager {

  private static NamedClusterManager instance = new NamedClusterManager();
  
  private Map<IMetaStore, MetaStoreFactory<NamedCluster>> factoryMap = new HashMap<IMetaStore, MetaStoreFactory<NamedCluster>>();
  
  private NamedCluster clusterTemplate;
  
  private NamedClusterManager() {
  }

  public static NamedClusterManager getInstance() {
    return instance;
  }

  private MetaStoreFactory<NamedCluster> getMetaStoreFactory( IMetaStore metastore ) {
    if ( factoryMap.get( metastore ) == null ) {
     factoryMap.put( metastore, new MetaStoreFactory<NamedCluster>( NamedCluster.class, metastore, PentahoDefaults.NAMESPACE ) );
    }
    return factoryMap.get( metastore );
  }
  
  /**
   * This method returns the named cluster template used to configure new NamedClusters.
   * 
   * Note that this method returns a clone (deep) of the template.
   * 
   * @return the NamedCluster template
   */  
  public NamedCluster getClusterTemplate() {
    if ( clusterTemplate == null ) {
      clusterTemplate = new NamedCluster();
      clusterTemplate.setName( "" );
      clusterTemplate.setHdfsHost( "localhost" );
      clusterTemplate.setHdfsPort( "8020" );
      clusterTemplate.setHdfsUsername( "user" );
      clusterTemplate.setHdfsPassword( "password" );
      clusterTemplate.setJobTrackerHost( "localhost" );
      clusterTemplate.setJobTrackerPort( "8032" );
      clusterTemplate.setZooKeeperHost( "localhost" );
      clusterTemplate.setZooKeeperPort( "2181" );
      clusterTemplate.setOozieUrl( "http://localhost:8080/oozie" );
    }
    return clusterTemplate.clone();
  }

  /**
   * This method will set the cluster template used when creating new NamedClusters
   * 
   * @param clusterTemplate the NamedCluster template to set
   */  
  public void setClusterTemplate( NamedCluster clusterTemplate ) {
    this.clusterTemplate = clusterTemplate;
  }
  
  /**
   * Saves a named cluster in the provided IMetaStore
   * 
   * @param namedCluster the NamedCluster to save
   * @param metastore the IMetaStore to operate with
   * @throws MetaStoreException
   */
  public void create( NamedCluster namedCluster, IMetaStore metastore ) throws MetaStoreException {
    MetaStoreFactory<NamedCluster> factory = getMetaStoreFactory( metastore );
    factory.saveElement( namedCluster );
  }
  
  /**
   * Reads a NamedCluster from the provided IMetaStore
   * 
   * @param clusterName the name of the NamedCluster to load
   * @param metastore the IMetaStore to operate with
   * @return the NamedCluster that was loaded
   * @throws MetaStoreException
   */
  public NamedCluster read( String clusterName, IMetaStore metastore ) throws MetaStoreException {
    MetaStoreFactory<NamedCluster> factory = getMetaStoreFactory( metastore );
    return factory.loadElement( clusterName );
  }


  /**
   * Updates a NamedCluster in the provided IMetaStore
   * 
   * @param namedCluster the NamedCluster to update
   * @param metastore the IMetaStore to operate with
   * @throws MetaStoreException
   */
  public void update( NamedCluster namedCluster, IMetaStore metastore ) throws MetaStoreException {
    MetaStoreFactory<NamedCluster> factory = getMetaStoreFactory( metastore );
    factory.deleteElement( namedCluster.getName() );
    factory.saveElement( namedCluster );
  }  

  /**
   * Deletes a NamedCluster from the provided IMetaStore
   * 
   * @param clusterName the NamedCluster to delete
   * @param metastore the IMetaStore to operate with
   * @throws MetaStoreException
   */
  public void delete( String clusterName, IMetaStore metastore ) throws MetaStoreException {
    MetaStoreFactory<NamedCluster> factory = getMetaStoreFactory( metastore );
    factory.deleteElement( clusterName );
  }  
  
  /**
   * This method lists the NamedCluster in the given IMetaStore
   * 
   * @param metastore the IMetaStore to operate with
   * @return the list of NamedClusters in the provided IMetaStore
   * @throws MetaStoreException
   */
  public List<NamedCluster> list( IMetaStore metastore ) throws MetaStoreException {
    MetaStoreFactory<NamedCluster> factory = getMetaStoreFactory( metastore );
    return factory.getElements();
  }  
  
  /**
   * This method returns the list of NamedCluster names in the IMetaStore
   * 
   * @param metastore the IMetaStore to operate with
   * @return the list of NamedCluster names (Strings)
   * @throws MetaStoreException
   */
  public List<String> listNames( IMetaStore metastore ) throws MetaStoreException {
    MetaStoreFactory<NamedCluster> factory = getMetaStoreFactory( metastore );
    return factory.getElementNames();
  }   
  
  /**
   * This method checks if the NamedCluster exists in the metastore
   * 
   * @param clusterName the name of the NamedCluster to check
   * @param metastore the IMetaStore to operate with
   * @return true if the NamedCluster exists in the given metastore
   * @throws MetaStoreException
   */
  public boolean contains( String clusterName, IMetaStore metastore ) throws MetaStoreException {
    if ( metastore == null ) {
      return false;
    }
    for ( String name : listNames( metastore ) ) {
      if ( name.equals( clusterName ) ) {
        return true;
      }
    }
    return false;
  }
  
  /**
   * This method generates the URL from the specific NamedCluster using the specified scheme.
   *
   * @param scheme
   *          the name of the scheme to use to create the URL
   * @param clusterName
   *          the name of the NamedCluster to use to create the URL
   * @return the generated URL from the specific NamedCluster or null if an error occurs
   */
  private String generateURL( String scheme, String clusterName, IMetaStore metastore, VariableSpace variableSpace ) {
    String clusterURL = null;
    try {
      if ( !Const.isEmpty( scheme ) && !Const.isEmpty( clusterName ) && metastore != null ) {
        NamedCluster namedCluster = read( clusterName, metastore );
        if ( namedCluster != null ) {
          String ncHostname = null, ncPort = null, ncUsername = null, ncPassword = null;

          if ( scheme.equals( HadoopSpoonPlugin.HDFS_SCHEME ) ) {
            ncHostname = namedCluster.getHdfsHost() != null ? namedCluster.getHdfsHost() : "";
            ncPort = namedCluster.getHdfsPort() != null ? namedCluster.getHdfsPort() : "";
            ncUsername = namedCluster.getHdfsUsername() != null ? namedCluster.getHdfsUsername() : "";
            ncPassword = namedCluster.getHdfsPassword() != null ? namedCluster.getHdfsPassword() : "";
          }

          if ( variableSpace != null ) {
            variableSpace.initializeVariablesFrom( namedCluster.getParentVariableSpace() );
            if ( StringUtil.isVariable( ncHostname ) ) {
              ncHostname =
                  variableSpace.getVariable( StringUtil.getVariableName( ncHostname ) ) != null ? variableSpace
                      .environmentSubstitute( ncHostname ) : null;
            }
            if ( StringUtil.isVariable( ncPort ) ) {
              ncPort =
                  variableSpace.getVariable( StringUtil.getVariableName( ncPort ) ) != null ? variableSpace
                      .environmentSubstitute( ncPort ) : null;
            }
            if ( StringUtil.isVariable( ncUsername ) ) {
              ncUsername =
                  variableSpace.getVariable( StringUtil.getVariableName( ncUsername ) ) != null ? variableSpace
                      .environmentSubstitute( ncUsername ) : null;
            }
            if ( StringUtil.isVariable( ncPassword ) ) {
              ncPassword =
                  variableSpace.getVariable( StringUtil.getVariableName( ncPassword ) ) != null ? variableSpace
                      .environmentSubstitute( ncPassword ) : null;
            }
          }

          ncHostname = ncHostname != null ? ncHostname.trim() : "";
          ncPort = ncPort != null ? ncPort.trim() : "";
          ncUsername = ncUsername != null ? ncUsername.trim() : "";
          ncPassword = ncPassword != null ? ncPassword.trim() : "";

          UrlFileName file =
              new UrlFileName( scheme, ncHostname, Integer.parseInt( ncPort ), -1, ncUsername, ncPassword, null, null,
                  null );
          clusterURL = file.getURI();
          if ( clusterURL.endsWith( "/" ) ) {
            clusterURL = clusterURL.substring( 0, clusterURL.lastIndexOf( "/" ) );
          }
        }
      }
    } catch ( Exception e ) {
      clusterURL = null;
    }
    return clusterURL;
  }

  /**
   * This method performs the root URL substitution with the URL of the specified NamedCluster
   *
   * @param clusterName
   *          the NamedCluster to use to generate the URL for the substitution
   * @param incomingURL
   *          the URL whose root will be replaced
   * @param scheme
   *          the scheme to be used to generate the URL of the specified NamedCluster
   * @return the generated URL or the incoming URL if an error occurs
   */
  public String processURLsubstitution( String clusterName, String incomingURL,
      String scheme, IMetaStore metastore, VariableSpace variableSpace ) {
    String outgoingURL = null;
    try {
      String clusterURL = generateURL( scheme, clusterName, metastore, variableSpace );
      if ( clusterURL == null ) {
        outgoingURL = incomingURL;
      } else if ( incomingURL.equals("/") ) {
        outgoingURL = clusterURL;
      } else if ( clusterURL != null ) {
        String noVariablesURL = incomingURL.replaceAll( "[${}]", "/" );
        
        String fullyQualifiedIncomingURL = incomingURL;
        if ( !incomingURL.startsWith( scheme ) ) {
          fullyQualifiedIncomingURL = clusterURL + incomingURL;
          noVariablesURL = clusterURL + incomingURL.replaceAll( "[${}]", "/" );
        }
        
        UrlFileNameParser parser = new UrlFileNameParser();
        FileName fileName = parser.parseUri( null, null, noVariablesURL );
        String root = fileName.getRootURI();
        String path = fullyQualifiedIncomingURL.substring( root.length() - 1 );
        StringBuffer buffer = new StringBuffer();
        buffer.append( clusterURL );
        buffer.append( path );
        outgoingURL = buffer.toString();
      }
    } catch ( Exception e ) {
      outgoingURL = null;
    }
    return outgoingURL;
  }
  
  public NamedCluster getNamedClusterByName( String namedCluster, IMetaStore metastore ) {
    if ( metastore == null ) {
      return null;
    }
    try {
      List<NamedCluster> namedClusters = list( metastore );
      for ( NamedCluster nc : namedClusters ) {
        if ( nc.getName().equals( namedCluster ) ) {
          return nc;
        }
      }
    } catch ( MetaStoreException e ) {
      return null;
    }
    return null;
  }
}
