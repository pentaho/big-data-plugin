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

package com.pentaho.di.plugins.catalog.metadata;

import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.util.serialization.BaseSerializingMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

@Step( id = "CatalogWriteMetadata", image = "CatalogEditMetadata.svg", name = "Write Metadata",
        description = "Write Metadata to Catalog", categoryDescription = "Catalog" )
@InjectionSupported( localizationPrefix = "Catalog.Metadata.Injection" )
public class CatalogWriteMetadataMeta extends BaseSerializingMeta implements StepMetaInterface {

  @Injection( name = "CONNECTION" )
  private String connection;

  @Injection( name = "RESOURCE_ID" )
  private String resourceId;

  @Injection( name = "RESOURCE_FROM_PREVIOUS" )
  private boolean resourceFromPrevious;

  @Injection( name = "PASS_THROUGH_FIELD" )
  private boolean passThroughFields;

  @Injection( name = "INPUT_FIELD_NAME" )
  private String inputFieldName;

  @Injection( name = "DESCRIPTION" )
  private String description;

  @Injection( name = "TAGS" )
  private String tags;

  private int selectedIndex = 0;

  @Override
  public void setDefault() {
    connection = "";
    resourceId = "";
    resourceFromPrevious = Boolean.FALSE;
    passThroughFields = Boolean.FALSE;
    inputFieldName = "";
    description = "";
    tags = "";
    selectedIndex = 0;
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int i, TransMeta transMeta, Trans trans ) {
    return new CatalogWriteMetadata( stepMeta, stepDataInterface, i, transMeta, trans );
  }

  public String getConnection() {
    return connection;
  }

  public void setConnection( String connection ) {
    this.connection = connection;
  }

  public String getResourceId() {
    return resourceId;
  }

  public void setResourceId( String resourceId ) {
    this.resourceId = resourceId;
  }

  public boolean getResourceFromPrevious() {
    return resourceFromPrevious;
  }

  public void setResourceFromPrevious( boolean resourceFromPrevious ) {
    this.resourceFromPrevious = resourceFromPrevious;
  }

  public boolean getPassThroughFields() {
    return passThroughFields;
  }

  public void setPassThroughFields( boolean passThroughFields ) {
    this.passThroughFields = passThroughFields;
  }

  public String getInputFieldName() {
    return inputFieldName;
  }

  public void setInputFieldName( String inputFieldName ) {
    this.inputFieldName = inputFieldName;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription( String description ) {
    this.description = description;
  }

  public String getTags() {
    return tags;
  }

  public void setTags( String tags ) {
    this.tags = tags;
  }

  public int getSelectedIndex() {
    return selectedIndex;
  }

  public void setSelectedIndex( int selectedIndex ) {
    this.selectedIndex = selectedIndex;
  }

  @Override
  public StepDataInterface getStepData() {
    return new CatalogWriteMetadataData();
  }

  public String getDialogClassName() {
    return "com.pentaho.di.plugins.catalog.metadata.CatalogWriteMetadataDialog";
  }
}
