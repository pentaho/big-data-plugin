package org.pentaho.di.job.entries.oozie;

import org.pentaho.hadoop.shim.ShimVersion;
import org.pentaho.oozie.shim.api.OozieClient;
import org.pentaho.oozie.shim.api.OozieClientFactory;

public class OozieClientFactoryImpl implements OozieClientFactory {

  @Override
  public ShimVersion getVersion() {
    return new ShimVersion( 0, 0 );
  }

  @Override
  public OozieClient create( String oozieUrl ) {
    return new OozieClientImpl( new org.apache.oozie.client.OozieClient( oozieUrl ) );
  }

}
