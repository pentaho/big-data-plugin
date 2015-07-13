package org.pentaho.big.data.api.cluster;

import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.util.List;

/**
 * Created by bryan on 6/24/15.
 */
public interface NamedClusterService {
  /**
   * This method returns the named cluster template used to configure new NamedClusters.
   * <p/>
   * Note that this method returns a clone (deep) of the template.
   *
   * @return the NamedCluster template
   */
  NamedCluster getClusterTemplate();

  /**
   * This method will set the cluster template used when creating new NamedClusters
   *
   * @param clusterTemplate the NamedCluster template to set
   */
  void setClusterTemplate( NamedCluster clusterTemplate );

  /**
   * Saves a named cluster in the provided IMetaStore
   *
   * @param namedCluster the NamedCluster to save
   * @param metastore    the IMetaStore to operate with
   * @throws MetaStoreException
   */
  void create( NamedCluster namedCluster, IMetaStore metastore ) throws MetaStoreException;

  /**
   * Reads a NamedCluster from the provided IMetaStore
   *
   * @param clusterName the name of the NamedCluster to load
   * @param metastore   the IMetaStore to operate with
   * @return the NamedCluster that was loaded
   * @throws MetaStoreException
   */
  NamedCluster read( String clusterName, IMetaStore metastore ) throws MetaStoreException;

  /**
   * Updates a NamedCluster in the provided IMetaStore
   *
   * @param namedCluster the NamedCluster to update
   * @param metastore    the IMetaStore to operate with
   * @throws MetaStoreException
   */
  void update( NamedCluster namedCluster, IMetaStore metastore ) throws MetaStoreException;

  /**
   * Deletes a NamedCluster from the provided IMetaStore
   *
   * @param clusterName the NamedCluster to delete
   * @param metastore   the IMetaStore to operate with
   * @throws MetaStoreException
   */
  void delete( String clusterName, IMetaStore metastore ) throws MetaStoreException;

  /**
   * This method lists the NamedCluster in the given IMetaStore
   *
   * @param metastore the IMetaStore to operate with
   * @return the list of NamedClusters in the provided IMetaStore
   * @throws MetaStoreException
   */
  List<NamedCluster> list( IMetaStore metastore ) throws MetaStoreException;

  /**
   * This method returns the list of NamedCluster names in the IMetaStore
   *
   * @param metastore the IMetaStore to operate with
   * @return the list of NamedCluster names (Strings)
   * @throws MetaStoreException
   */
  List<String> listNames( IMetaStore metastore ) throws MetaStoreException;

  /**
   * This method checks if the NamedCluster exists in the metastore
   *
   * @param clusterName the name of the NamedCluster to check
   * @param metastore   the IMetaStore to operate with
   * @return true if the NamedCluster exists in the given metastore
   * @throws MetaStoreException
   */
  boolean contains( String clusterName, IMetaStore metastore ) throws MetaStoreException;

  NamedCluster getNamedClusterByName( String namedCluster, IMetaStore metastore );
}
