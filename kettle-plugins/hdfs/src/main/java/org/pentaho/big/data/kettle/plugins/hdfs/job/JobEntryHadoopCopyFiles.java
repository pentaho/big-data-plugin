/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.big.data.kettle.plugins.hdfs.job;

import com.google.common.annotations.VisibleForTesting;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.JobEntry;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.job.entries.copyfiles.JobEntryCopyFiles;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;

import java.util.Map;

@JobEntry( id = "HadoopCopyFilesPlugin", image = "HDM.svg", name = "HadoopCopyFilesPlugin.Name",
  description = "HadoopCopyFilesPlugin.Description",
  categoryDescription = "i18n:org.pentaho.di.job:JobCategory.Category.BigData",
  i18nPackageName = "org.pentaho.di.job.entries.hadoopcopyfiles",
  documentationUrl = "mk-95pdia003/pdi-job-entries/hadoop-copy-files" )
public class JobEntryHadoopCopyFiles extends JobEntryCopyFiles {

  public static final String S3_SOURCE_FILE = "S3-SOURCE-FILE-";
  public static final String S3_DEST_FILE = "S3-DEST-FILE-";
  private final NamedClusterService namedClusterService;
  private final RuntimeTestActionService runtimeTestActionService;
  private final RuntimeTester runtimeTester;

  public JobEntryHadoopCopyFiles( NamedClusterService namedClusterService,
                                  RuntimeTestActionService runtimeTestActionService, RuntimeTester runtimeTester ) {
    this.namedClusterService = namedClusterService;
    this.runtimeTestActionService = runtimeTestActionService;
    this.runtimeTester = runtimeTester;
  }

  public String loadURL( String url, String ncName, IMetaStore metastore, Map mappings ) {
    NamedCluster c = namedClusterService.getNamedClusterByName( ncName, metastore );
    String origUrl;
    String pref = null;
    if ( url != null && url.indexOf( SOURCE_URL ) > -1 ) {
      origUrl = url;
      url = origUrl.substring( origUrl.indexOf( "-", origUrl.indexOf( SOURCE_URL ) + SOURCE_URL.length() ) + 1 );
      pref = origUrl.substring( 0, origUrl.indexOf( "-", origUrl.indexOf( SOURCE_URL ) + SOURCE_URL.length() ) + 1 );
    } else if ( url != null && url.indexOf( DEST_URL ) > -1 ) {
      origUrl = url;
      url = origUrl.substring( origUrl.indexOf( "-", origUrl.indexOf( DEST_URL ) + DEST_URL.length() ) + 1 );
      pref = origUrl.substring( 0, origUrl.indexOf( "-", origUrl.indexOf( DEST_URL ) + DEST_URL.length() ) + 1 );
    }
    if ( c != null ) {
      url = c.processURLsubstitution( url, metastore, getVariables() );
    }
    if ( pref != null ) {
      url = pref + url;
    }
    if ( !Const.isEmpty( ncName ) && !Const.isEmpty( url ) ) {
      mappings.put( url, ncName );
    }
    return url;
  }

  @VisibleForTesting
  @Override protected VariableSpace getVariables() {
    return super.getVariables();
  }

  public NamedClusterService getNamedClusterService() {
    return namedClusterService;
  }

  public RuntimeTestActionService getRuntimeTestActionService() {
    return runtimeTestActionService;
  }

  public RuntimeTester getRuntimeTester() {
    return runtimeTester;
  }
}
