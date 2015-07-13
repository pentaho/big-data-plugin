package org.pentaho.big.data.api.cluster;

import org.pentaho.di.core.variables.VariableSpace;

/**
 * Created by bryan on 6/24/15.
 */
public interface NamedCluster extends Cloneable, VariableSpace {
  String getName();

  void setName( String name );

  void replaceMeta( NamedCluster nc );

  String getHdfsHost();

  void setHdfsHost( String hdfsHost );

  String getHdfsPort();

  void setHdfsPort( String hdfsPort );

  String getHdfsUsername();

  void setHdfsUsername( String hdfsUsername );

  String getHdfsPassword();

  void setHdfsPassword( String hdfsPassword );

  String getJobTrackerHost();

  void setJobTrackerHost( String jobTrackerHost );

  String getJobTrackerPort();

  void setJobTrackerPort( String jobTrackerPort );

  String getZooKeeperHost();

  void setZooKeeperHost( String zooKeeperHost );

  String getZooKeeperPort();

  void setZooKeeperPort( String zooKeeperPort );

  String getOozieUrl();

  void setOozieUrl( String oozieUrl );

  long getLastModifiedDate();

  void setLastModifiedDate( long lastModifiedDate );

  boolean isMapr();

  void setMapr( boolean mapr );

  NamedCluster clone();
}
