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
import org.pentaho.big.data.impl.cluster.NamedClusterManager;
import org.pentaho.di.core.annotations.JobEntry;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.job.entries.copyfiles.JobEntryCopyFiles;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;
import org.pentaho.runtime.test.action.impl.RuntimeTestActionServiceImpl;
import org.pentaho.runtime.test.impl.RuntimeTesterImpl;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

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

  public JobEntryHadoopCopyFiles() {
    this.namedClusterService = NamedClusterManager.getInstance();
    this.runtimeTestActionService = RuntimeTestActionServiceImpl.getInstance();
    this.runtimeTester = RuntimeTesterImpl.getInstance();
    this.fileFolderUrlMappings = new HashMap<>();
  }

  /**
   * Hold mapping to go back to unresolved or original URL stored in the xml.
   * <p/>
   * Mapping legend:
   * <ul>
   *   <li><b>Key:</b> return value from {@link #loadURL(String, String, IMetaStore, Map)}</li>
   *   <li><b>Value:</b> stored URL from fields ( {@link #SOURCE_FILE_FOLDER } and {@link #DESTINATION_FILE_FOLDER} ) or first parameter
   *    * of {@link #loadURL(String, String, IMetaStore, Map)}</li>
   * </ul>
   */
  protected final Map<String, String> fileFolderUrlMappings;

  public JobEntryHadoopCopyFiles( NamedClusterService namedClusterService,
                                  RuntimeTestActionService runtimeTestActionService, RuntimeTester runtimeTester ) {
    this.namedClusterService = namedClusterService;
    this.runtimeTestActionService = runtimeTestActionService;
    this.runtimeTester = runtimeTester;
    this.fileFolderUrlMappings = new HashMap<>();
  }

  @Override
  public String loadURL( String url, String ncName, IMetaStore metastore, Map mappings ) {
    NamedCluster c = namedClusterService.getNamedClusterByName( ncName, metastore );
    String origUrl = url;
    boolean saveArgumentUrl = false;
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
      String valueBeforeCall = url;
      url = c.processURLsubstitution( url, metastore, getVariables() );
      saveArgumentUrl = !Objects.equals( valueBeforeCall, url );
    }
    if ( pref != null ) {
      url = pref + url;
    }

    if ( saveArgumentUrl ) {
      fileFolderUrlMappings.put( url, origUrl );
    }

    return super.loadURL( url, ncName, metastore, mappings );
  }

  /**
   * Preserve the original URL input argument from {@link #loadURL(String, String, IMetaStore, Map)} and don't save the
   * "resolved" URL, otherwise call normal logic from super class.
   * @see JobEntryCopyFiles#loadURL(String, String, IMetaStore, Map)
   * @param url
   * @param ncName
   * @param metastore
   * @param mappings
   * @return original URL if it has changed otherwise, the result from super class
   */
  @Override
  public String saveURL( String url, String ncName, IMetaStore metastore, Map<String, String> mappings ) {
    return !Objects.isNull( url ) && fileFolderUrlMappings.containsKey( url )
      ? fileFolderUrlMappings.get( url )
      : super.saveURL( url, ncName, metastore, mappings );
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
