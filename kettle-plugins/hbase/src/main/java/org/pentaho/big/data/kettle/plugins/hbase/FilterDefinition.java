/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

import org.pentaho.hadoop.shim.api.hbase.mapping.ColumnFilter;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionTypeConverter;

public class FilterDefinition {

  @Injection( name = "ALIAS", group = "FILTER" )
  private String alias;

  @Injection( name = "FIELD_TYPE", group = "FILTER" )
  private String fieldType;

  @Injection( name = "COMPARISON_TYPE", group = "FILTER", converter = ComparisonTypeConverter.class )
  private ColumnFilter.ComparisonType comparisonType;

  @Injection( name = "SIGNED_COMPARISON", group = "FILTER" )
  private boolean signedComparison;

  @Injection( name = "COMPARISON_VALUE", group = "FILTER" )
  private String constant;

  @Injection( name = "FORMAT", group = "FILTER" )
  private String format;

  public String getAlias() {
    return alias;
  }

  public void setAlias( String alias ) {
    this.alias = alias;
  }

  public String getFieldType() {
    return fieldType;
  }

  public void setFieldType( String fieldType ) {
    this.fieldType = fieldType;
  }

  public ColumnFilter.ComparisonType getComparisonType() {
    return comparisonType;
  }

  public void setComparisonType( ColumnFilter.ComparisonType comparisonType ) {
    this.comparisonType = comparisonType;
  }

  public boolean isSignedComparison() {
    return signedComparison;
  }

  public void setSignedComparison( boolean signedComparison ) {
    this.signedComparison = signedComparison;
  }

  public String getConstant() {
    return constant;
  }

  public void setConstant( String constant ) {
    this.constant = constant;
  }

  public String getFormat() {
    return format;
  }

  public void setFormat( String format ) {
    this.format = format;
  }

  public static class ComparisonTypeConverter extends InjectionTypeConverter {
    @Override
    public ColumnFilter.ComparisonType string2enum( Class<?> enumClass, String value ) throws KettleValueException {
      return ColumnFilter.ComparisonType.stringToOpp( value );
    }
  }
}
