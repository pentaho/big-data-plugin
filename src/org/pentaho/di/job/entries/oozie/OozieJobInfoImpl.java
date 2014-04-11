package org.pentaho.di.job.entries.oozie;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.pentaho.oozie.shim.api.OozieClientException;
import org.pentaho.oozie.shim.api.OozieJob;

public class OozieJobInfoImpl implements OozieJob {
  private final String id;
  private final OozieClient oozieClient;

  public OozieJobInfoImpl( String id, OozieClient oozieClient ) {
    this.id = id;
    this.oozieClient = oozieClient;
  }

  @Override
  public boolean didSucceed() throws OozieClientException {
    try {
      return oozieClient.getJobInfo( id ).getStatus().equals( WorkflowJob.Status.SUCCEEDED );
    } catch ( org.apache.oozie.client.OozieClientException e ) {
      throw new OozieClientException( e.getCause(), e.getErrorCode() );
    }
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public String getJobLog() throws OozieClientException {
    try {
      return oozieClient.getJobLog( id );
    } catch ( org.apache.oozie.client.OozieClientException e ) {
      throw new OozieClientException( e.getCause(), e.getErrorCode() );
    }
  }

  @Override
  public boolean isRunning() throws OozieClientException {
    try {
      return oozieClient.getJobInfo( id ).getStatus().equals( WorkflowJob.Status.RUNNING );
    } catch ( org.apache.oozie.client.OozieClientException e ) {
      throw new OozieClientException( e.getCause(), e.getErrorCode() );
    }
  }

}
