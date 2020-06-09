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

package com.pentaho.di.plugins.catalog.api.entities;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.pentaho.di.plugins.catalog.api.entities.payload.AbstractField;

import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties( ignoreUnknown = true )
@JsonInclude( JsonInclude.Include.NON_NULL )
public class DataResource {
  private String key;
  private String type;
  private Double timeOfCreation;
  private Double timeOfLastChange;
  private Double score;
  private String resourceType;
  private String resourcePath;
  private String dataSourceKey;
  private String dataSourceUri;
  private Boolean canAddOrRemoveResourceFromDataset;
  private Boolean canCreateHiveTableOrView;
  private String dataSourceType;
  private Boolean lastPartitionProfile;
  private String dataSourceName;
  private Double timeOfResourceAccess;
  private Double timeOfResourceChange;
  private String fileFormat;
  private String fileFormatDisplay;
  private Double resourceSize;
  private String owner;
  private Double curationCount;
  private Double usageCount;
  private Double subscribersCount;
  private Boolean permittedToRead;
  private Boolean dataPermitted;
  private String description;
  private Double fieldCount;
  private List<AbstractField> fields;
  private String separator;
  private Boolean header;

  public DataResource() {
    fields = new ArrayList<>();
  }

  public String getDescription() {
    return description;
  }

  public void setDescription( String description ) {
    this.description = description;
  }

  public String getKey() {
    return key;
  }

  public void setKey( String key ) {
    this.key = key;
  }

  public String getType() {
    return type;
  }

  public void setType( String type ) {
    this.type = type;
  }

  public Double getTimeOfCreation() {
    return timeOfCreation;
  }

  public void setTimeOfCreation( Double timeOfCreation ) {
    this.timeOfCreation = timeOfCreation;
  }

  public Double getTimeOfLastChange() {
    return timeOfLastChange;
  }

  public void setTimeOfLastChange( Double timeOfLastChange ) {
    this.timeOfLastChange = timeOfLastChange;
  }

  public Double getScore() {
    return score;
  }

  public void setScore( Double score ) {
    this.score = score;
  }

  public String getResourceType() {
    return resourceType;
  }

  public void setResourceType( String resourceType ) {
    this.resourceType = resourceType;
  }

  public String getResourcePath() {
    return resourcePath;
  }

  public void setResourcePath( String resourcePath ) {
    this.resourcePath = resourcePath;
  }

  public String getDataSourceKey() {
    return dataSourceKey;
  }

  public void setDataSourceKey( String dataSourceKey ) {
    this.dataSourceKey = dataSourceKey;
  }

  public String getDataSourceUri() {
    return dataSourceUri;
  }

  public void setDataSourceUri( String dataSourceUri ) {
    this.dataSourceUri = dataSourceUri;
  }

  public Boolean isCanAddOrRemoveResourceFromDataset() {
    return canAddOrRemoveResourceFromDataset;
  }

  public void setCanAddOrRemoveResourceFromDataset( Boolean canAddOrRemoveResourceFromDataset ) {
    this.canAddOrRemoveResourceFromDataset = canAddOrRemoveResourceFromDataset;
  }

  public Boolean isCanCreateHiveTableOrView() {
    return canCreateHiveTableOrView;
  }

  public void setCanCreateHiveTableOrView( Boolean canCreateHiveTableOrView ) {
    this.canCreateHiveTableOrView = canCreateHiveTableOrView;
  }

  public String getDataSourceType() {
    return dataSourceType;
  }

  public void setDataSourceType( String dataSourceType ) {
    this.dataSourceType = dataSourceType;
  }

  public Boolean isLastPartitionProfile() {
    return lastPartitionProfile;
  }

  public void setLastPartitionProfile( Boolean lastPartitionProfile ) {
    this.lastPartitionProfile = lastPartitionProfile;
  }

  public String getDataSourceName() {
    return dataSourceName;
  }

  public void setDataSourceName( String dataSourceName ) {
    this.dataSourceName = dataSourceName;
  }

  public Double getTimeOfResourceAccess() {
    return timeOfResourceAccess;
  }

  public void setTimeOfResourceAccess( Double timeOfResourceAccess ) {
    this.timeOfResourceAccess = timeOfResourceAccess;
  }

  public Double getTimeOfResourceChange() {
    return timeOfResourceChange;
  }

  public void setTimeOfResourceChange( Double timeOfResourceChange ) {
    this.timeOfResourceChange = timeOfResourceChange;
  }

  public String getFileFormat() {
    return fileFormat;
  }

  public void setFileFormat( String fileFormat ) {
    this.fileFormat = fileFormat;
  }

  public String getFileFormatDisplay() {
    return fileFormatDisplay;
  }

  public void setFileFormatDisplay( String fileFormatDisplay ) {
    this.fileFormatDisplay = fileFormatDisplay;
  }

  public Double getResourceSize() {
    return resourceSize;
  }

  public void setResourceSize( Double resourceSize ) {
    this.resourceSize = resourceSize;
  }

  public String getOwner() {
    return owner;
  }

  public void setOwner( String owner ) {
    this.owner = owner;
  }

  public Double getCurationCount() {
    return curationCount;
  }

  public void setCurationCount( Double curationCount ) {
    this.curationCount = curationCount;
  }

  public Double getUsageCount() {
    return usageCount;
  }

  public void setUsageCount( Double usageCount ) {
    this.usageCount = usageCount;
  }

  public Double getSubscribersCount() {
    return subscribersCount;
  }

  public void setSubscribersCount( Double subscribersCount ) {
    this.subscribersCount = subscribersCount;
  }

  public Boolean isPermittedToRead() {
    return permittedToRead;
  }

  public void setPermittedToRead( Boolean permittedToRead ) {
    this.permittedToRead = permittedToRead;
  }

  public Boolean isDataPermitted() {
    return dataPermitted;
  }

  public void setDataPermitted( Boolean dataPermitted ) {
    this.dataPermitted = dataPermitted;
  }

  public Double getFieldCount() {
    return fieldCount;
  }

  public void setFieldCount( Double fieldCount ) {
    this.fieldCount = fieldCount;
  }

  public List<AbstractField> getFields() {
    return fields;
  }

  public Boolean hasHeader() {
    return header;
  }

  public void setHeader( Boolean header ) {
    this.header = header;
  }

  public String getSeparator() {
    return separator;
  }

  public void setSeparator( String separator ) {
    this.separator = separator;
  }

  public void setFields( List<AbstractField> fields ) {
    this.fields = fields;
  }
}
