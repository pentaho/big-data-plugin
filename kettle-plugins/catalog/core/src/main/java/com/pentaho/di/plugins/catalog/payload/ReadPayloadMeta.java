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

package com.pentaho.di.plugins.catalog.payload;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.annotations.Step;
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
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.file.BaseFileField;
import org.pentaho.di.trans.steps.file.BaseFileInputFiles;
import org.pentaho.metastore.api.IMetaStore;

import java.util.ArrayList;
import java.util.List;


/**
 * Skeleton for PDI Step plugin.
 */
@Step( id = "Read Payload", image = "SearchCatalogStep.svg", name = "Read Payload", description = "Read The Payload",
  categoryDescription = "Catalog" )
@InjectionSupported( localizationPrefix = "Catalog.Injection.", groups = { "RESOURCE_FIELDS" } )
public class ReadPayloadMeta extends BaseSerializingMeta implements StepMetaInterface {

  private static Class<?> PKG = ReadPayload.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  public static final String DATA_RESOURCE = "Data Resource";
  public static final String DATA_SOURCE = "Data Source";
  private BaseFileInputFiles inputFiles;
  BaseFileField[] inputFields;

  public ReadPayloadMeta() {
    super();
    inputFiles = new BaseFileInputFiles();
    inputFields = new BaseFileField[0];
  }

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

  @Injection( name = "ID_FIELD" )
  private String idField;

  @Injection( name = "DATA_SOURCE_FIELD" )
  private String dataSourceField;

  @Injection( name = "VIRTUAL_FOLDER_FIELD" )
  private String virtualFolderField;


  @Override
  @SuppressWarnings( "squid:S2975" )
  public Object clone() {
    ReadPayloadMeta retval = (ReadPayloadMeta) super.clone();
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
    idField = "";
    dataSourceField = "";
    virtualFolderField = "";
  }

  public void getFields( RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, Repository repository, IMetaStore metaStore ) throws KettleStepException {

    if ( !inputFiles.passingThruFields ) {
      rowMeta.clear();
    } else {
      if ( info != null ) {
        boolean found = false;
        for ( int i = 0; i < info.length && !found; i++ ) {
          if ( info[i] != null ) {
            rowMeta.mergeRowMeta( info[i], origin );
            found = true;
          }
        }
      }
    }

    try {

      for ( int i = 0; i < getInputFields().length; i++ ) {
        BaseFileField field = getInputFields()[i];

        int type = field.getType();
        if ( type == ValueMetaInterface.TYPE_NONE ) {
          type = ValueMetaInterface.TYPE_STRING;
        }

        ValueMetaInterface val = ValueMetaFactory.createValueMeta( field.getName(), type );
        val.setLength( field.getLength() );
        val.setPrecision( field.getPrecision() );
        val.setOrigin( origin );
        val.setConversionMask( field.getFormat() );
        val.setDecimalSymbol( field.getDecimalSymbol() );
        val.setGroupingSymbol( field.getGroupSymbol() );
        val.setCurrencySymbol( field.getCurrencySymbol() );
        val.setTrimType( field.getTrimType() );

        rowMeta.addValueMeta( val );
      }

      if ( StringUtils.isNotBlank( idField ) ) {
        ValueMetaInterface v = ValueMetaFactory.createValueMeta( "idField", ValueMetaInterface.TYPE_STRING );
        rowMeta.addValueMeta( v );
      }

      if ( StringUtils.isNotBlank( dataSourceField ) ) {
        ValueMetaInterface v = ValueMetaFactory.createValueMeta( "dataSourceField", ValueMetaInterface.TYPE_STRING );
        rowMeta.addValueMeta( v );
      }

      if ( StringUtils.isNotBlank( virtualFolderField ) ) {
        ValueMetaInterface v = ValueMetaFactory.createValueMeta( "virtualFolderField", ValueMetaInterface.TYPE_STRING );
        rowMeta.addValueMeta( v );
      }

    } catch ( Exception e ) {
      throw new KettleStepException( e );
    }
  }

  public void check( List<CheckResultInterface> remarks, TransMeta transMeta,
                     StepMeta stepMeta, RowMetaInterface prev, String[] input, String[] output,
                     RowMetaInterface info, VariableSpace space, Repository repository,
                     IMetaStore metaStore ) {
    if ( prev == null || prev.size() == 0 ) {
      remarks.add( new CheckResult( CheckResultInterface.TYPE_RESULT_WARNING,
        BaseMessages.getString( PKG, "ReadPayloadMeta.CheckResult.NotReceivingFields" ), stepMeta ) );
    } else {
      remarks.add( new CheckResult( CheckResultInterface.TYPE_RESULT_OK,
        BaseMessages.getString( PKG, "ReadPayloadMeta.CheckResult.StepRecevingData", prev.size() + "" ), stepMeta ) );
    }

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      remarks.add( new CheckResult( CheckResultInterface.TYPE_RESULT_OK,
        BaseMessages.getString( PKG, "ReadPayloadMeta.CheckResult.StepRecevingData2" ), stepMeta ) );
    } else {
      remarks.add( new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR,
        BaseMessages.getString( PKG, "ReadPayloadMeta.CheckResult.NoInputReceivedFromOtherSteps" ), stepMeta ) );
    }
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr,
                                Trans trans ) {
    return new ReadPayload( stepMeta, stepDataInterface, cnr, tr, trans );
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

  public BaseFileField[] getInputFields() {
    return inputFields;
  }

  public void setInputFields( BaseFileField[] inputFields ) {
    this.inputFields = inputFields;
  }

  public String getIdField() {
    return idField;
  }

  public void setIdField( String idField ) {
    this.idField = idField;
  }

  public String getDataSourceField() {
    return dataSourceField;
  }

  public void setDataSourceField( String dataSourceField ) {
    this.dataSourceField = dataSourceField;
  }

  public String getVirtualFolderField() {
    return virtualFolderField;
  }

  public void setVirtualFolderField( String virtualFolderField ) {
    this.virtualFolderField = virtualFolderField;
  }

  public StepDataInterface getStepData() {
    return new ReadPayloadData();
  }

  public String getDialogClassName() {
    return "com.pentaho.di.plugins.catalog.payload.ReadPayloadDialog";
  }
}

