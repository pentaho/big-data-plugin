/*!
 * HITACHI VANTARA PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2020 Hitachi Vantara. All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Hitachi Vantara and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Hitachi Vantara and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Hitachi Vantara is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Hitachi Vantara,
 * explicitly covering such access.
 */

package com.pentaho.di.plugins.catalog.search;

import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionDeep;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.util.serialization.BaseSerializingMeta;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.metastore.api.IMetaStore;

import java.util.ArrayList;
import java.util.List;


/**
 * Skeleton for PDI Step plugin.
 */
@Step( id = "Search Catalog", image = "SearchCatalogStep.svg", name = "Search Catalog", description = "Search the Catalog",
  categoryDescription = "Catalog" )
@InjectionSupported( localizationPrefix = "Catalog.Injection.", groups = { "RESOURCE_FIELDS" } )
public class SearchCatalogMeta extends BaseSerializingMeta implements StepMetaInterface {

  private static Class<?> catalogClass = SearchCatalog.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  public static final String DATA_RESOURCE = "Data Resource";
  public static final String DATA_SOURCE = "Data Source";
  protected static final String[] DATA_TYPES = new String[] {
    DATA_RESOURCE, DATA_SOURCE
  };

  @InjectionDeep
  private List<ResourceField> resourceFields = new ArrayList<>();

  @Injection( name = "CONNECTION" )
  private String connection;

  @Injection( name = "KEYWORD" )
  private String keyword;

  @Injection( name = "TAGS" )
  private String tags;

  @Injection( name = "VIRTUAL_FOLDERS" )
  private String virtualFolders;

  @Injection( name = "DATA_SOURCES" )
  private String dataSources;

  @Injection( name = "RESOURCE_TYPE" )
  private String resourceType;

  @Injection( name = "FILE_SIZE" )
  private String fileSize;

  @Injection( name = "FILE_FORMAT" )
  private String fileFormat;

  @Injection( name = "ADVANCED_QUERY" )
  private String advancedQuery;

  @Injection( name = "SELECTED_INDEX" )
  private Integer selectedIndex = 0;

  public SearchCatalogMeta() {
    super(); // allocate BaseStepMeta
  }

  @Override
  @SuppressWarnings( "squid:S2975" )
  public Object clone() {
    SearchCatalogMeta retval = (SearchCatalogMeta) super.clone();
    retval.resourceFields = resourceFields;
    return retval;
  }

  @Override
  public void setDefault() {
    resourceFields = new ArrayList<>();
    connection = "";
    keyword = "";
    tags = "";
    virtualFolders = "";
    dataSources = "";
    resourceType = "";
    fileSize = "";
    fileFormat = "";
    advancedQuery = "";
    selectedIndex = 0;
  }

  public void getFields( RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, Repository repository, IMetaStore metaStore ) throws KettleStepException {
    // Default: nothing changes to rowMeta

    try {
      ValueMetaInterface v = ValueMetaFactory.createValueMeta( "key", ValueMetaInterface.TYPE_STRING );
      rowMeta.addValueMeta( v );
      v = ValueMetaFactory.createValueMeta( "type", ValueMetaInterface.TYPE_STRING );
      rowMeta.addValueMeta( v );
      v = ValueMetaFactory.createValueMeta( "timeOfCreation", ValueMetaInterface.TYPE_NUMBER );
      rowMeta.addValueMeta( v );
      v = ValueMetaFactory.createValueMeta( "timeOfLastChange", ValueMetaInterface.TYPE_NUMBER );
      rowMeta.addValueMeta( v );
      v = ValueMetaFactory.createValueMeta( "resourceType", ValueMetaInterface.TYPE_STRING );
      rowMeta.addValueMeta( v );
      v = ValueMetaFactory.createValueMeta( "resourcePath", ValueMetaInterface.TYPE_STRING );
      rowMeta.addValueMeta( v );
      v = ValueMetaFactory.createValueMeta( "dataSourceKey", ValueMetaInterface.TYPE_STRING );
      rowMeta.addValueMeta( v );
      v = ValueMetaFactory.createValueMeta( "dataSourceUri", ValueMetaInterface.TYPE_STRING );
      rowMeta.addValueMeta( v );
      v = ValueMetaFactory.createValueMeta( "dataSourceType", ValueMetaInterface.TYPE_STRING );
      rowMeta.addValueMeta( v );
      v = ValueMetaFactory.createValueMeta( "dataSourceName", ValueMetaInterface.TYPE_STRING );
      rowMeta.addValueMeta( v );
      v = ValueMetaFactory.createValueMeta( "timeOfResourceAccess", ValueMetaInterface.TYPE_NUMBER );
      rowMeta.addValueMeta( v );
      v = ValueMetaFactory.createValueMeta( "timeOfResourceChange", ValueMetaInterface.TYPE_NUMBER );
      rowMeta.addValueMeta( v );
      v = ValueMetaFactory.createValueMeta( "fileFormat", ValueMetaInterface.TYPE_STRING );
      rowMeta.addValueMeta( v );
      v = ValueMetaFactory.createValueMeta( "fileFormatDisplay", ValueMetaInterface.TYPE_STRING );
      rowMeta.addValueMeta( v );
      v = ValueMetaFactory.createValueMeta( "resourceSize", ValueMetaInterface.TYPE_NUMBER );
      rowMeta.addValueMeta( v );
      v = ValueMetaFactory.createValueMeta( "owner", ValueMetaInterface.TYPE_STRING );
      rowMeta.addValueMeta( v );

    } catch ( KettlePluginException kpe ) {
      throw new KettleStepException( "Unable to load fields" );
    }
  }

  public void check( List<CheckResultInterface> remarks, TransMeta transMeta,
                     StepMeta stepMeta, RowMetaInterface prev, String[] input, String[] output,
                     RowMetaInterface info, VariableSpace space, Repository repository,
                     IMetaStore metaStore ) {
    if ( prev == null || prev.size() == 0 ) {
      remarks.add( new CheckResult( CheckResultInterface.TYPE_RESULT_WARNING,
        BaseMessages.getString( catalogClass, "SearchCatalogMeta.CheckResult.NotReceivingFields" ), stepMeta ) );
    } else {
      remarks.add( new CheckResult( CheckResultInterface.TYPE_RESULT_OK,
        BaseMessages.getString( catalogClass, "SearchCatalogMeta.CheckResult.StepRecevingData", prev.size() + "" ),
        stepMeta ) );
    }

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      remarks.add( new CheckResult( CheckResultInterface.TYPE_RESULT_OK,
        BaseMessages.getString( catalogClass, "SearchCatalogMeta.CheckResult.StepRecevingData2" ), stepMeta ) );
    } else {
      remarks.add( new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR,
        BaseMessages.getString( catalogClass, "SearchCatalogMeta.CheckResult.NoInputReceivedFromOtherSteps" ), stepMeta ) );
    }
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr,
                                Trans trans ) {
    return new SearchCatalog( stepMeta, stepDataInterface, cnr, tr, trans );
  }

  public List<ResourceField> getResourceFields() {
    return resourceFields;
  }

  public void setResourceFields( List<ResourceField> resourceFields ) {
    this.resourceFields = resourceFields;
  }

  public String getConnection() {
    return connection;
  }

  public void setConnection( String connection ) {
    this.connection = connection;
  }

  public String getKeyword() {
    return keyword;
  }

  public void setKeyword( String keyword ) {
    this.keyword = keyword;
  }

  public String getTags() {
    return tags;
  }

  public void setTags( String tags ) {
    this.tags = tags;
  }

  public String getVirtualFolders() {
    return virtualFolders;
  }

  public void setVirtualFolders( String virtualFolders ) {
    this.virtualFolders = virtualFolders;
  }

  public String getDataSources() {
    return dataSources;
  }

  public void setDataSources( String dataSources ) {
    this.dataSources = dataSources;
  }

  public String getResourceType() {
    return resourceType;
  }

  public void setResourceType( String resourceType ) {
    this.resourceType = resourceType;
  }

  public String getFileSize() {
    return fileSize;
  }

  public void setFileSize( String fileSize ) {
    this.fileSize = fileSize;
  }

  public String getFileFormat() {
    return fileFormat;
  }

  public void setFileFormat( String fileFormat ) {
    this.fileFormat = fileFormat;
  }

  public String getAdvancedQuery() {
    return advancedQuery;
  }

  public void setAdvancedQuery( String advancedQuery ) {
    this.advancedQuery = advancedQuery;
  }

  public Integer getSelectedIndex() {
    return selectedIndex;
  }

  public void setSelectedIndex( Integer selectedIndex ) {
    this.selectedIndex = selectedIndex;
  }

  public StepDataInterface getStepData() {
    return new SearchCatalogData();
  }

  public String getDialogClassName() {
    return "com.pentaho.di.plugins.catalog.search.SearchCatalogDialog";
  }
}

