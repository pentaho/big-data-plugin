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

package org.pentaho.di.core.namedcluster;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.provider.url.UrlFileName;
import org.apache.commons.vfs2.provider.url.UrlFileNameParser;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class NamedClusterManager {
  private static final NamedClusterManager instance = new NamedClusterManager( new MetaStoreFactoryFactory() );

  private final MetaStoreFactoryFactory metaStoreFactoryFactory;

  private final Map<IMetaStore, MetaStoreFactory<NamedCluster>> factoryMap;

  private NamedCluster clusterTemplate;

  @VisibleForTesting
  NamedClusterManager( MetaStoreFactoryFactory metaStoreFactoryFactory ) {
    this.metaStoreFactoryFactory = metaStoreFactoryFactory;
    factoryMap = new HashMap<>();
  }

  public static NamedClusterManager getInstance() {
    return instance;
  }

  private MetaStoreFactory<NamedCluster> getMetaStoreFactory( IMetaStore metastore ) {
    return factoryMap.computeIfAbsent( metastore, k -> metaStoreFactoryFactory.createFactory( metastore ) );
  }

  /**
   * This method returns the named cluster template used to configure new NamedClusters.
   *
   * Note that this method returns a clone (deep) of the template.
   *
   * @return the NamedCluster template
   */
  public NamedCluster getClusterTemplate() {
    String localHost = "localhost";
    if ( clusterTemplate == null ) {
      clusterTemplate = new NamedCluster();
      clusterTemplate.setName( "" );
      clusterTemplate.setHdfsHost( localHost );
      clusterTemplate.setHdfsPort( "8020" );
      clusterTemplate.setHdfsUsername( "user" );
      clusterTemplate.setHdfsPassword( clusterTemplate.encodePassword( "password" ) );
      clusterTemplate.setJobTrackerHost( localHost );
      clusterTemplate.setJobTrackerPort( "8032" );
      clusterTemplate.setZooKeeperHost( localHost );
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
    List<NamedCluster> namedClusters = list( metastore );
    for ( NamedCluster nc : namedClusters ) {
      if ( namedCluster.getName().equals( nc.getName() ) ) {
        factory.deleteElement( nc.getName() );
        factory.saveElement( namedCluster );
      }
    }

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
   * This method lists the NamedCluster in the given IMetaStore.  If an exception is thrown when parsing the data for
   * a given NamedCluster, the namedCluster will be skipped a processing will continue without logging the exception.
   * This methods serves to limit the frequency of any exception logs that would otherwise occur.
   *
   * @param metastore the IMetaStore to operate with
   * @return the list of NamedClusters in the provided IMetaStore
   * @throws MetaStoreException
   */
  public List<NamedCluster> list( IMetaStore metastore ) throws MetaStoreException {
    MetaStoreFactory<NamedCluster> factory = getMetaStoreFactory( metastore );
    List<MetaStoreException> exceptionList = new ArrayList<>();
    return factory.getElements( true, exceptionList );
  }

  /**
   * This method lists the NamedClusters in the given IMetaStore.  If an exception is thrown when parsing the data for
   * a given NamedCluster.  The exception will be added to the exceptionList, but list generation will continue.
   *
   * @param metastore the IMetaStore to operate with
   * @return the list of NamedClusters in the provided IMetaStore
   * @throws MetaStoreException
   */
  public List<NamedCluster> list( IMetaStore metastore, List<MetaStoreException> exceptionList )
    throws MetaStoreException {
    MetaStoreFactory<NamedCluster> factory = getMetaStoreFactory( metastore );
    return factory.getElements( true, exceptionList );
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
  @VisibleForTesting
  String generateURL( String scheme, String clusterName, IMetaStore metastore, VariableSpace variableSpace ) {
    String clusterURL = null;
    try {
      if ( !Utils.isEmpty( scheme ) && !Utils.isEmpty( clusterName ) && metastore != null ) {
        NamedCluster namedCluster = read( clusterName, metastore );
        if ( namedCluster != null ) {
          String ncHostname = Optional.ofNullable( namedCluster.getHdfsHost() ).orElse( "" );
          String ncPort = Optional.ofNullable( namedCluster.getHdfsPort() ).orElse( "" );
          String ncUsername = Optional.ofNullable( namedCluster.getHdfsUsername() ).orElse( "" );
          String ncPassword = Optional.ofNullable( namedCluster.getHdfsPassword() ).orElse( "" );

          if ( variableSpace != null ) {
            variableSpace.initializeVariablesFrom( namedCluster.getParentVariableSpace() );
            scheme = getVariableValue( scheme, variableSpace );
            ncHostname = getVariableValue( ncHostname, variableSpace );
            ncPort = getVariableValue( ncPort, variableSpace );
            ncUsername = getVariableValue( ncUsername, variableSpace );
            ncPassword = getVariableValue( ncPassword, variableSpace );
          }

          ncHostname = Optional.ofNullable( ncHostname ).orElse( "" ).trim();
          if ( ncPort == null || Utils.isEmpty( ncPort.trim() ) ) {
            ncPort = "-1";
          }
          ncPort = ncPort.trim();
          ncUsername = Optional.ofNullable( ncUsername ).orElse( "" ).trim();
          ncPassword = Optional.ofNullable( ncPassword ).orElse( "" ).trim();

          UrlFileName file =
              new UrlFileName( scheme, ncHostname, Integer.parseInt( ncPort ), -1, ncUsername, ncPassword, null, null,
                  null );
          clusterURL = file.getURI();
          if ( clusterURL.endsWith( "/" ) ) {
            clusterURL = clusterURL.substring( 0, clusterURL.lastIndexOf( '/' ) );
          }
        }
      }
    } catch ( Exception e ) {
      clusterURL = null;
    }
    return clusterURL;
  }

  /**
   * This method checks a value to see if it is a variable and, if it is, gets the value from a variable space.
   * @param variableName The String to check if it is a variable
   * @param variableSpace The variable space to check for values
   * @return The value of the variable within the variable space if found, null if not found or the original value if is not a variable.
   */
  private String getVariableValue( String variableName, VariableSpace variableSpace ) {
    if ( StringUtil.isVariable( variableName ) ) {
      return variableSpace.getVariable( StringUtil.getVariableName( variableName ) ) != null ? variableSpace
                      .environmentSubstitute( variableName ) : null;
    }
    return variableName;
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
    String clusterURL = null;
    if ( !scheme.equals( NamedCluster.MAPRFS_SCHEME ) ) {
      clusterURL = generateURL( scheme, clusterName, metastore, variableSpace );
    }
    try {
      if ( clusterURL == null ) {
        outgoingURL = incomingURL;
      } else if ( incomingURL.equals( "/" ) ) {
        outgoingURL = clusterURL;
      } else {
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
        StringBuilder buffer = new StringBuilder();
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

  static class MetaStoreFactoryFactory {
    MetaStoreFactory<NamedCluster> createFactory( IMetaStore metaStore ) {
      return new MetaStoreFactory<>( NamedCluster.class, metaStore, PentahoDefaults.NAMESPACE );
    }
  }
}
