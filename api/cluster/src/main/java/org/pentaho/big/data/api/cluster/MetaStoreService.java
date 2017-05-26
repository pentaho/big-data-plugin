package org.pentaho.big.data.api.cluster;

import org.pentaho.metastore.api.IMetaStore;

/**
 * Created by dstepanov on 11/05/17.
 */
public interface MetaStoreService {

  IMetaStore getMetaStore();
}
