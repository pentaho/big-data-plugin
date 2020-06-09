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

import com.pentaho.di.plugins.catalog.api.CatalogClient;
import com.pentaho.di.plugins.catalog.api.entities.DataSource;
import com.pentaho.di.plugins.catalog.provider.CatalogDetails;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.connections.ConnectionManager;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.i18n.BaseMessages;
import com.pentaho.di.plugins.catalog.api.entities.DataResource;
import com.pentaho.di.plugins.catalog.api.entities.search.Facet;
import com.pentaho.di.plugins.catalog.api.entities.search.PagingCriteria;
import com.pentaho.di.plugins.catalog.api.entities.search.SearchCriteria;
import com.pentaho.di.plugins.catalog.api.entities.search.SearchResult;
import com.pentaho.di.plugins.catalog.api.entities.search.SortBySpecs;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Describe your step plugin.
 */
public class SearchCatalog extends BaseStep implements StepInterface {

  public static final String SCORE = "score";
  public static final String DATA_RESOURCE = "data_resource";
  public static final String ADVANCED = "ADVANCED";
  private static Class<?> catalogMetaClass = SearchCatalogMeta.class;
  // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
  Supplier<ConnectionManager> connectionManagerSupplier = ConnectionManager::getInstance;

  private SearchCatalogMeta meta;
  private SearchCatalogData data;

  public SearchCatalog( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                        Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }


  /**
   * Initialize and do work where other steps need to wait for...
   *
   * @param smi The metadata to work with
   * @param sdi The data to initialize
   */
  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {

    if ( !super.init( smi, sdi ) ) {
      return false;
    }

    meta = (SearchCatalogMeta) smi;
    data = (SearchCatalogData) sdi;

    CatalogDetails catalogDetails = (CatalogDetails) connectionManagerSupplier.get()
      .getConnectionDetails( CatalogDetails.CATALOG, environmentSubstitute( meta.getConnection() ) );

    URL url;
    try {
      url = new URL( catalogDetails.getUrl() );
    } catch ( MalformedURLException mue ) {
      return false;
    }

    data.setCatalogClient( new CatalogClient( url.getHost(), String.valueOf( url.getPort() ),
      url.getProtocol().equals( CatalogClient.HTTPS ) ) );
    data.getCatalogClient().getAuthentication()
      .login( environmentSubstitute( catalogDetails.getUsername() ),
        environmentSubstitute( catalogDetails.getPassword() ) );

    return ( data.getCatalogClient().getSessionId() != null );
  }

  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    if ( first ) {
      first = false;
      data.setOutputRowMeta( new RowMeta() );
      meta.getFields( data.getOutputRowMeta(), getStepname(), null, null, this, repository, metaStore );
    }

    switch ( meta.getSelectedIndex() ) {
      case 0:
        doList();
        break;
      case 1:
        doSearch();
        break;
      case 2:
        doAdvancedSearch();
        break;
      default:
        doList();
        break;
    }

    if ( checkFeedback( getLinesRead() ) && log.isBasic() ) {
      logBasic( BaseMessages.getString( catalogMetaClass, "CatalogStep.Log.LineNumber" ) + getLinesRead() );
    }

    setOutputDone();
    return false;
  }

  private void putDataResource( DataResource dataResource ) throws KettleException {
    Object[] outputRowData = new Object[] {};
    int index = 0;
    outputRowData = RowDataUtil.addValueData( outputRowData, index++, dataResource.getKey() );
    outputRowData = RowDataUtil.addValueData( outputRowData, index++, dataResource.getType() );
    outputRowData = RowDataUtil.addValueData( outputRowData, index++, dataResource.getTimeOfCreation() );
    outputRowData = RowDataUtil.addValueData( outputRowData, index++, dataResource.getTimeOfLastChange() );
    outputRowData = RowDataUtil.addValueData( outputRowData, index++, dataResource.getResourceType() );
    outputRowData = RowDataUtil.addValueData( outputRowData, index++, dataResource.getResourcePath() );
    outputRowData = RowDataUtil.addValueData( outputRowData, index++, dataResource.getDataSourceKey() );
    outputRowData = RowDataUtil.addValueData( outputRowData, index++, dataResource.getDataSourceUri() );
    outputRowData = RowDataUtil.addValueData( outputRowData, index++, dataResource.getDataSourceType() );
    outputRowData = RowDataUtil.addValueData( outputRowData, index++, dataResource.getDataSourceName() );
    outputRowData = RowDataUtil.addValueData( outputRowData, index++, dataResource.getTimeOfResourceAccess() );
    outputRowData = RowDataUtil.addValueData( outputRowData, index++, dataResource.getTimeOfResourceChange() );
    outputRowData = RowDataUtil.addValueData( outputRowData, index++, dataResource.getFileFormat() );
    outputRowData = RowDataUtil.addValueData( outputRowData, index++, dataResource.getFileFormatDisplay() );
    outputRowData = RowDataUtil.addValueData( outputRowData, index++, dataResource.getResourceSize() );
    outputRowData = RowDataUtil.addValueData( outputRowData, index, dataResource.getOwner() );
    putRow( data.getOutputRowMeta(), outputRowData );
  }

  private void doList() throws KettleException {
    for ( ResourceField resourceField : meta.getResourceFields() ) {
      if ( resourceField.getId() != null ) {
        DataResource dataResource = data.getCatalogClient().getDataResources().read( resourceField.getId() );
        putDataResource( dataResource );
      }
    }
  }

  private void doSearch() throws KettleException {
    SearchCriteria.SearchCriteriaBuilder searchCriteriaBuilder = new SearchCriteria.SearchCriteriaBuilder();

    if ( StringUtils.isNotEmpty( meta.getKeyword() ) ) {
      searchCriteriaBuilder.searchPhrase( meta.getKeyword() );
    }

    searchCriteriaBuilder.addFacet( Facet.RESOURCE_TAGS, meta.getTags() );
    searchCriteriaBuilder.addFacet( Facet.VIRTUAL_FOLDERS, meta.getVirtualFolders() );
    searchCriteriaBuilder.addFacet( Facet.DATA_SOURCES, meta.getDataSources() );
    searchCriteriaBuilder.addFacet( Facet.RESOURCE_TYPE, meta.getResourceType() );
    searchCriteriaBuilder.addFacet( Facet.FILE_SIZE, meta.getFileSize() );
    searchCriteriaBuilder.addFacet( Facet.FILE_FORMAT, meta.getFileFormat() );

    searchCriteriaBuilder.pagingCriteria( new PagingCriteria( 0, 25 ) )
      .sortBySpecs( Collections.singletonList( new SortBySpecs( SCORE, false ) ) )
      .entityScope( Collections.singletonList( DATA_RESOURCE ) )
      .searchType( ADVANCED )
      .preformedQuery( false )
      .build();

    SearchResult result = data.getCatalogClient().getSearch().doNew( searchCriteriaBuilder.build() );
    if ( result.getEntities() != null ) {
      Map<String, List<DataResource>> dataSources =
        result.getEntities().stream().collect( Collectors.groupingBy( DataResource::getDataSourceKey ) );

      for ( Map.Entry<String, List<DataResource>> entry : dataSources.entrySet() ) {
        DataSource dataSource = data.getCatalogClient().getDataSources().read( entry.getKey() );
        for ( DataResource dataResource : entry.getValue() ) {
          dataResource.setDataSourceUri( dataSource.getHdfsUri() );
          dataResource.setDataSourceName( dataSource.getName() );
          putDataResource( dataResource );
        }
      }
    }
  }

  private void doAdvancedSearch() throws KettleException {
    String search = Const.NVL( meta.getAdvancedQuery(), "" );
    SearchResult result = data.getCatalogClient().getSearch().doNew( search );
    if ( result.getEntities() != null ) {
      Map<String, List<DataResource>> dataSources =
        result.getEntities().stream().collect( Collectors.groupingBy( DataResource::getDataSourceKey ) );

      for ( Map.Entry<String, List<DataResource>> entry : dataSources.entrySet() ) {
        DataSource dataSource = data.getCatalogClient().getDataSources().read( entry.getKey() );
        for ( DataResource dataResource : entry.getValue() ) {
          dataResource.setDataSourceUri( dataSource.getHdfsUri() );
          dataResource.setDataSourceName( dataSource.getName() );
          putDataResource( dataResource );
        }
      }
    }
  }
}

