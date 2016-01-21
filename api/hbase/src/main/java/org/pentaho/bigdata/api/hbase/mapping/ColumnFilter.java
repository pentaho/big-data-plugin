/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

package org.pentaho.bigdata.api.hbase.mapping;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;

/**
 * Created by bryan on 1/19/16.
 */
public interface ColumnFilter {
  String getFieldAlias();

  void setFieldAlias( String alias );

  String getFieldType();

  void setFieldType( String type );

  ColumnFilter.ComparisonType getComparisonOperator();

  void setComparisonOperator( ColumnFilter.ComparisonType c );

  boolean getSignedComparison();

  void setSignedComparison( boolean signed );

  String getConstant();

  void setConstant( String constant );

  String getFormat();

  void setFormat( String format );

  void appendXML( StringBuilder buff );

  void saveRep( Repository rep, ObjectId id_transformation, ObjectId id_step, int filterNum ) throws
    KettleException;

  enum ComparisonType {
    EQUAL( "=" ),
    GREATER_THAN( ">" ),
    GREATER_THAN_OR_EQUAL( ">=" ),
    LESS_THAN( "<" ),
    LESS_THAN_OR_EQUAL( "<=" ),
    NOT_EQUAL( "!=" ),
    SUBSTRING( "Substring" ),
    REGEX( "Regular expression" ),
    PREFIX( "Starts from" );

    private final String m_stringVal;

    ComparisonType( String name ) {
      this.m_stringVal = name;
    }

    public static String[] getAllOperators() {
      String[] ops =
        new String[] { ColumnFilter.ComparisonType.EQUAL.toString(), ColumnFilter.ComparisonType.NOT_EQUAL.toString(),
          ColumnFilter.ComparisonType.GREATER_THAN.toString(),
          ColumnFilter.ComparisonType.GREATER_THAN_OR_EQUAL.toString(),
          ColumnFilter.ComparisonType.LESS_THAN.toString(), ColumnFilter.ComparisonType.LESS_THAN_OR_EQUAL.toString(),
          ColumnFilter.ComparisonType.SUBSTRING.toString(), ColumnFilter.ComparisonType.PREFIX.toString(),
          ColumnFilter.ComparisonType.REGEX.toString() };
      return ops;
    }

    public static String[] getStringOperators() {
      String[] ops =
        new String[] { ColumnFilter.ComparisonType.SUBSTRING.toString(), ColumnFilter.ComparisonType.PREFIX.toString(),
          ColumnFilter.ComparisonType.REGEX.toString() };
      return ops;
    }

    public static String[] getNumericOperators() {
      String[] ops =
        new String[] { ColumnFilter.ComparisonType.EQUAL.toString(), ColumnFilter.ComparisonType.NOT_EQUAL.toString(),
          ColumnFilter.ComparisonType.GREATER_THAN.toString(),
          ColumnFilter.ComparisonType.GREATER_THAN_OR_EQUAL.toString(),
          ColumnFilter.ComparisonType.LESS_THAN.toString(), ColumnFilter.ComparisonType.LESS_THAN_OR_EQUAL.toString() };
      return ops;
    }

    public static ColumnFilter.ComparisonType stringToOpp( String opp ) {
      ColumnFilter.ComparisonType c = null;
      ColumnFilter.ComparisonType[] arr$ = ColumnFilter.ComparisonType.values();
      int len$ = arr$.length;

      for ( int i$ = 0; i$ < len$; ++i$ ) {
        ColumnFilter.ComparisonType t = arr$[ i$ ];
        if ( t.toString().equals( opp ) ) {
          c = t;
          break;
        }
      }

      return c;
    }

    public String toString() {
      return this.m_stringVal;
    }
  }
}
