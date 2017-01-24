/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
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
package org.pentaho.big.data.kettle.plugins.hbase;

import java.util.ArrayList;
import java.util.List;

import org.pentaho.bigdata.api.hbase.mapping.ColumnFilter;
import org.pentaho.bigdata.api.hbase.mapping.ColumnFilterFactory;
import org.pentaho.bigdata.api.hbase.mapping.Mapping;
import org.pentaho.bigdata.api.hbase.mapping.MappingFactory;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.w3c.dom.Node;

public class HBaseDataLoadSaveUtil {

  public static Mapping loadMapping( Node stepNode, MappingFactory mappingFactory ) throws KettleXMLException {
    Mapping mapping = mappingFactory.createMapping();
    if ( !mapping.loadXML( stepNode ) ) {
      mapping = null;
    }
    return mapping;
  }

  public static Mapping loadMapping( Repository rep, ObjectId id_step, MappingFactory mappingFactory ) throws KettleException {
    Mapping mapping = mappingFactory.createMapping();
    if ( !mapping.readRep( rep, id_step ) ) {
      mapping = null;
    }
    return mapping;
  }

  public static List<ColumnFilter> loadFilters( Node stepNode, ColumnFilterFactory colFtFactory ) {
    List<ColumnFilter> colFilters = null;
    Node filters = XMLHandler.getSubNode( stepNode, "column_filters" );
    int nrFilters = XMLHandler.countNodes( filters, "filter" );
    if ( nrFilters > 0 ) {
      colFilters = new ArrayList<ColumnFilter>( nrFilters );
      for ( int i = 0; i < nrFilters; i++ ) {
        Node filterNode = XMLHandler.getSubNodeByNr( filters, "filter", i );
        colFilters.add( colFtFactory.createFilter( filterNode ) );
      }
    }
    return colFilters;
  }

  public static List<ColumnFilter> loadFilters( Repository rep, ObjectId id_step, ColumnFilterFactory colFtFactory ) throws KettleException {
    List<ColumnFilter> colFilters = null;
    int nrFilters = rep.countNrStepAttributes( id_step, "cf_comparison_opp" );
    if ( nrFilters > 0 ) {
      colFilters = new ArrayList<ColumnFilter>( nrFilters );
      for ( int i = 0; i < nrFilters; i++ ) {
        colFilters.add( colFtFactory.createFilter( rep, i, id_step ) );
      }
    }
    return colFilters;
  }

}
