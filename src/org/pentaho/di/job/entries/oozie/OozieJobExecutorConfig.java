/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.job.entries.oozie;

import java.util.ArrayList;
import java.util.List;

import org.pentaho.di.core.namedcluster.NamedClusterManager;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.job.BlockableJobConfig;
import org.pentaho.di.job.JobEntryMode;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.PropertyEntry;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.ui.xul.XulEventSource;
import org.pentaho.ui.xul.stereotype.Bindable;

/**
 * Model for the Oozie Job Executor
 * 
 * User: RFellows Date: 6/4/12
 */
public class OozieJobExecutorConfig extends BlockableJobConfig implements XulEventSource, Cloneable {

  public static final String OOZIE_WORKFLOW = "oozieWorkflow";
  public static final String OOZIE_URL = "oozieUrl";
  public static final String NAMED_CONFIGURATION = "namedConfiguration";
  public static final String OOZIE_WORKFLOW_CONFIG = "oozieWorkflowConfig";
  public static final String OOZIE_WORKFLOW_PROPERTIES = "oozieWorkflowProperties";
  public static final String MODE = "mode";

  private transient JobMeta jobMeta;
  
  private transient List<NamedCluster> namedClusters;
  private transient NamedCluster namedCluster = null; // selected
  private String clusterName; // saved (String)
  
  private String oozieUrl = null;
  private String oozieWorkflowConfig = null;
  private String oozieWorkflow = null;

  // coded to implementation not Interface for serialization purposes
  private ArrayList<PropertyEntry> workflowProperties = null;
  private String mode = null;

  public OozieJobExecutorConfig() {
  }
  
  @Bindable
  public String getOozieUrl() {
    return oozieUrl;
  }

  @Bindable
  public void setOozieUrl( String oozieUrl ) {
    String prev = this.oozieUrl;
    this.oozieUrl = oozieUrl;
    pcs.firePropertyChange( OOZIE_URL, prev, this.oozieUrl );
  }

  @Bindable
  public String getClusterName() {
    return clusterName;
  }

  @Bindable
  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }  
  
  @Bindable
  public NamedCluster getNamedCluster() {
    return namedCluster;
  }

  @Bindable
  public void setNamedCluster( NamedCluster namedCluster ) {
    this.namedCluster = namedCluster;
    if ( namedCluster != null ) {
      this.clusterName = namedCluster.getName();
      this.oozieUrl = namedCluster.getOozieUrl();
    }
  }  
  
  @Bindable
  public List<NamedCluster> getNamedClusters() {
    this.namedClusters = new ArrayList<NamedCluster>();
    if ( jobMeta != null ) {
      try {
        this.namedClusters = NamedClusterManager.getInstance().list( jobMeta.getMetaStore() );
      } catch (MetaStoreException e) {
        return namedClusters;
      }
    } 
    return namedClusters;
  }

  @Bindable
  public void setNamedClusters( List <NamedCluster> namedClusters ) {
    this.namedClusters = namedClusters;
  }
  
  @Bindable
  public String getOozieWorkflowConfig() {
    return oozieWorkflowConfig;
  }

  @Bindable
  public void setOozieWorkflowConfig( String oozieWorkflowConfig ) {
    String prev = this.oozieWorkflowConfig;
    this.oozieWorkflowConfig = oozieWorkflowConfig;
    pcs.firePropertyChange( OOZIE_WORKFLOW_CONFIG, prev, this.oozieWorkflowConfig );
  }

  @Bindable
  public String getOozieWorkflow() {
    return this.oozieWorkflow;
  }

  @Bindable
  public void setOozieWorkflow( String oozieWorkflow ) {
    String prev = this.oozieWorkflow;
    this.oozieWorkflow = oozieWorkflow;
    pcs.firePropertyChange( OOZIE_WORKFLOW, prev, oozieWorkflow );
  }

  /**
   * Workflow properties configured in the advanced mode of the Oozie Job Executor
   * 
   * @return
   */
  public List<PropertyEntry> getWorkflowProperties() {
    if ( workflowProperties == null ) {
      workflowProperties = new ArrayList<PropertyEntry>();
    }
    return workflowProperties;
  }

  public void setWorkflowProperties( List<PropertyEntry> workflowProperties ) {
    ArrayList<PropertyEntry> prev = this.workflowProperties;
    if ( workflowProperties instanceof ArrayList ) {
      this.workflowProperties = (ArrayList) workflowProperties;
    } else {
      this.workflowProperties = new ArrayList<PropertyEntry>( workflowProperties );
    }
    pcs.firePropertyChange( OOZIE_WORKFLOW_PROPERTIES, prev, workflowProperties );
  }

  public String getMode() {
    return mode;
  }

  public JobEntryMode getModeAsEnum() {
    try {
      return JobEntryMode.valueOf( getMode() );
    } catch ( Exception ex ) {
      // Not a valid ui mode, return the default
      return JobEntryMode.QUICK_SETUP;
    }
  }

  /**
   * Sets the mode based on the enum value
   * 
   * @param mode
   */
  public void setMode( JobEntryMode mode ) {
    setMode( mode.name() );
  }

  public void setMode( String mode ) {
    String old = this.mode;
    this.mode = mode;
    pcs.firePropertyChange( MODE, old, this.mode );
  }
  
  public void setJobMeta( JobMeta jobMeta ) {
    this.jobMeta = jobMeta;
  }  
  
}
