/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
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
