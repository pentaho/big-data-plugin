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

import com.pentaho.di.plugins.catalog.api.CatalogClient;
import com.pentaho.di.plugins.catalog.api.entities.DataResource;
import com.pentaho.di.plugins.catalog.api.entities.DataSource;
import com.pentaho.di.plugins.catalog.api.entities.payload.AbstractField;
import com.pentaho.di.plugins.catalog.api.entities.search.Facet;
import com.pentaho.di.plugins.catalog.api.entities.search.PagingCriteria;
import com.pentaho.di.plugins.catalog.api.entities.search.SearchCriteria;
import com.pentaho.di.plugins.catalog.api.entities.search.SearchResult;
import com.pentaho.di.plugins.catalog.api.entities.search.SortBySpecs;
import com.pentaho.di.plugins.catalog.provider.CatalogDetails;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.connections.ConnectionManager;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.io.File;
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
public class ReadPayload extends BaseStep implements StepInterface {

  private static final String SCORE = "score";
  private static final String DATA_RESOURCE = "data_resource";
  private static final String ADVANCED = "ADVANCED";
  private static Class<?> catalogMetaClass = ReadPayloadMeta.class;
  // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
  private Supplier<ConnectionManager> connectionManagerSupplier = ConnectionManager::getInstance;

  private ReadPayloadMeta meta;
  private ReadPayloadData data;

  public ReadPayload( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
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

    meta = (ReadPayloadMeta) smi;
    data = (ReadPayloadData) sdi;

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

  private void convertPayloadToRow( DataResource dataResource ) throws KettleException {

    String resourcePath = dataResource.getResourcePath().startsWith( File.separator )
            ? dataResource.getResourcePath().substring( 1 ) : dataResource.getResourcePath();

    String fullPath = dataResource.getDataSourceUri() + resourcePath;
    String dataStr = KettleVFS.getTextFileContent( fullPath, Const.XML_ENCODING );

    String[] lines = dataStr.split( "\\r?\\n" );
    int fieldCount = dataResource.getFieldCount().intValue();
    List<AbstractField> fields = dataResource.getFields();

    RowMetaInterface rowOutputMeta = new RowMeta();
    ValueMetaInterface v;

    for ( int i = 0; i < fieldCount; i++ ) {
      v = ValueMetaFactory.createValueMeta( fields.get( i ).getName(), ValueMetaInterface.TYPE_STRING );
      rowOutputMeta.addValueMeta( v );
    }

    if ( meta.getIdField() != null && !meta.getIdField().isEmpty() ) {
      v = ValueMetaFactory.createValueMeta( BaseMessages.getString( catalogMetaClass, "ReadPayloadStepDialog.ID.Label" ), ValueMetaInterface.TYPE_STRING );
      rowOutputMeta.addValueMeta( v );
    }

    if ( meta.getDataSourceField() != null && !meta.getDataSourceField().isEmpty() ) {
      v = ValueMetaFactory.createValueMeta( BaseMessages.getString( catalogMetaClass, "ReadPayloadStepDialog.DataSource.Label" ), ValueMetaInterface.TYPE_STRING );
      rowOutputMeta.addValueMeta( v );
    }

    if ( meta.getVirtualFolderField() != null && !meta.getVirtualFolderField().isEmpty() ) {
      v = ValueMetaFactory.createValueMeta( BaseMessages.getString( catalogMetaClass, "ReadPayloadStepDialog.VirtualFolder.Label" ), ValueMetaInterface.TYPE_STRING );
      rowOutputMeta.addValueMeta( v );
    }

    int initialLine = dataResource.hasHeader() ? 1 : 0;

    for ( int i = initialLine; i < lines.length; i++ ) {

      String currentLine = lines[i];
      String[] values = getCsvValues( fields, currentLine, dataResource.getSeparator() );
      Object[] outputRowData = getOutputRowData( values );
      putRow( rowOutputMeta, outputRowData );
    }
  }

  private void doList() throws KettleException {
    for ( ResourceField resourceField : meta.getResourceFields() ) {
      if ( resourceField.getId() != null ) {
        DataResource dataResource = data.getCatalogClient().getDataResources().read( resourceField.getId() );
        convertPayloadToRow( dataResource );
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
    searchAndConvert( result );
  }

  private void doAdvancedSearch() throws KettleException {
    String search = Const.NVL( meta.getAdvancedQuery(), "" );
    SearchResult result = data.getCatalogClient().getSearch().doNew( search );
    searchAndConvert( result );
  }

  private void searchAndConvert( SearchResult result ) throws KettleException {
    if ( result.getEntities() != null ) {
      Map<String, List<DataResource>> dataSources =
              result.getEntities().stream().collect( Collectors.groupingBy( DataResource::getDataSourceKey ) );

      for ( Map.Entry<String, List<DataResource>> entry : dataSources.entrySet() ) {
        DataSource dataSource = data.getCatalogClient().getDataSources().read( entry.getKey() );
        for ( DataResource dataResource : entry.getValue() ) {
          dataResource.setDataSourceUri( dataSource.getHdfsUri() );
          dataResource.setDataSourceName( dataSource.getName() );
          convertPayloadToRow( dataResource );
        }
      }
    }
  }

  private Object[] getOutputRowData( String[] values ) {
    int indexVal = 0;

    Object[] outputRowData = new Object[values.length];

    for ( String value : values ) {
      outputRowData = RowDataUtil.addValueData( outputRowData, indexVal++, value );
    }

    if ( meta.getIdField() != null && !meta.getIdField().isEmpty() ) {
      outputRowData = RowDataUtil.addValueData( outputRowData, indexVal++, meta.getIdField() );
    }

    if ( meta.getDataSourceField() != null && !meta.getDataSourceField().isEmpty() ) {
      outputRowData = RowDataUtil.addValueData( outputRowData, indexVal++, meta.getDataSourceField() );
    }

    if ( meta.getVirtualFolderField() != null && !meta.getVirtualFolderField().isEmpty() ) {
      outputRowData = RowDataUtil.addValueData( outputRowData, indexVal, meta.getVirtualFolderField() );
    }

    return outputRowData;
  }

  private String[] getCsvValues( List<AbstractField> fields, String line, String separator ) {

    String[] strings = new String[fields.size()];
    int fieldnr = 0;
    String pol;
    int pos = 0;
    int length = line.length();
    boolean dencl = false;

    String enclosure = "\"";

    int lengthEncl = enclosure.length();

    while ( pos < length ) {
      int from = pos;
      int next;

      boolean enclFound;

      // Is the field beginning with an enclosure?
      // "aa;aa";123;"aaa-aaa";000;...
      if ( line.substring( from, from + lengthEncl ).equalsIgnoreCase( enclosure ) ) {
        enclFound = true;
        int p = from + lengthEncl;

        boolean isEnclosure = p + lengthEncl < length && line.substring( p, p + lengthEncl ).equalsIgnoreCase( enclosure );
        boolean enclosureAfter = false;

        // Is it really an enclosure? See if it's not repeated twice or escaped!
        if ( ( isEnclosure ) && p < length - 1 ) {
          String strnext = line.substring( p + lengthEncl, p + 2 * lengthEncl );
          if ( strnext.equalsIgnoreCase( enclosure ) ) {
            p++;
            enclosureAfter = true;
            dencl = true;
          }
        }

        // Look for a closing enclosure!
        while ( ( !isEnclosure || enclosureAfter ) && p < line.length() ) {
          p++;
          enclosureAfter = false;
          isEnclosure = p + lengthEncl < length && line.substring( p, p + lengthEncl ).equals( enclosure );

          // Is it really an enclosure? See if it's not repeated twice or escaped!
          if ( ( isEnclosure ) && p < length - 1 ) {

            String strnext = line.substring( p + lengthEncl, p + 2 * lengthEncl );
            if ( strnext.equals( enclosure ) ) {
              p++;
              enclosureAfter = true;
              dencl = true;
            }
          }
        }
        next = ( p >= length ) ? p : p + lengthEncl;

      } else {
        enclFound = false;
        next = line.indexOf( separator, from );
      }
      if ( next == -1 ) {
        next = length;
      }

      if ( enclFound && ( ( from + lengthEncl ) <= ( next - lengthEncl ) ) ) {
        pol = line.substring( from + lengthEncl, next - lengthEncl );
      } else {
        pol = line.substring( from, next );
      }

      if ( dencl ) {
        StringBuilder sbpol = new StringBuilder( pol );
        int idx = sbpol.indexOf( enclosure + enclosure );
        while ( idx >= 0 ) {
          sbpol.delete( idx, idx + enclosure.length() );
          idx = sbpol.indexOf( enclosure + enclosure );
        }
        pol = sbpol.toString();
      }

      // Now add pol to the strings found!
      try {
        strings[fieldnr] = pol;
      } catch ( ArrayIndexOutOfBoundsException e ) {
        // In case we didn't allocate enough space.
        // This happens when you have less header values specified than there are actual values in the rows.
        // As this is "the exception" we catch and resize here.
        String[] newStrings = new String[strings.length];
        for ( int x = 0; x < strings.length; x++ ) {
          newStrings[x] = strings[x];
        }
        strings = newStrings;
      }

      pos = next + separator.length();
      fieldnr++;
    }
    if ( pos == length && fieldnr < strings.length ) {
      strings[fieldnr] = Const.EMPTY_STRING;
    }
    return strings;
  }
}
