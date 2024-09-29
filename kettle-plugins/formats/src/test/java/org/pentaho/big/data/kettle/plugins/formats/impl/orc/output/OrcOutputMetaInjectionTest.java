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

package org.pentaho.big.data.kettle.plugins.formats.impl.orc.output;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.pentaho.big.data.kettle.plugins.formats.impl.NamedClusterResolver;
import org.pentaho.big.data.kettle.plugins.formats.orc.output.OrcOutputField;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.injection.BaseMetadataInjectionTest;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.junit.rules.RestorePDIEngineEnvironment;
import org.pentaho.hadoop.shim.api.format.OrcSpec;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class OrcOutputMetaInjectionTest  extends BaseMetadataInjectionTest<OrcOutputMeta> {
  @ClassRule public static RestorePDIEngineEnvironment env = new RestorePDIEngineEnvironment();

  @Before
  public void setup() {
    NamedClusterResolver mockNamedClusterResolver = mock( NamedClusterResolver.class );
    setup( new OrcOutputMeta( mockNamedClusterResolver ) );
    OrcOutputField orcOutputField = new OrcOutputField();
    meta.setOutputFields( Arrays.asList( orcOutputField ) );
  }

  @Test
  public void test() throws Exception {

    check( "FILENAME", () -> meta.getFilename() );
    check( "OPTIONS_COMPRESS_SIZE", () -> meta.getCompressSize() );
    check( "OPTIONS_DATE_FORMAT", () -> meta.getDateTimeFormat() );
    check( "OPTIONS_DATE_IN_FILE_NAME", () -> meta.isDateInFileName() );
    check( "OPTIONS_ROWS_BETWEEN_ENTRIES", () -> meta.getRowsBetweenEntries() );
    check( "OPTIONS_STRIPE_SIZE", () -> meta.getStripeSize() );
    check( "OPTIONS_TIME_IN_FILE_NAME", () -> meta.isTimeInFileName() );
    check( "OVERRIDE_OUTPUT", () -> meta.isOverrideOutput() );

    check( "FIELD_DECIMAL_PRECISION", () -> meta.getOutputFields().get( 0 ).getPrecision() );
    check( "FIELD_DECIMAL_SCALE", () -> meta.getOutputFields().get( 0 ).getScale() );
    check( "FIELD_IF_NULL", () -> meta.getOutputFields().get( 0 ).getDefaultValue() );
    check( "FIELD_NAME", () -> meta.getOutputFields().get( 0 ).getPentahoFieldName() );
    check( "FIELD_NULLABLE", () -> meta.getOutputFields().get( 0 ).getAllowNull() );
    check( "FIELD_NULL_STRING", () -> meta.getOutputFields().get( 0 ).getAllowNull() );
    check( "FIELD_PATH", () -> meta.getOutputFields().get( 0 ).getFormatFieldName() );
    checkOrcTypes( "FIELD_TYPE", () -> meta.getOutputFields().get( 0 ).getFormatType(), OrcSpec.DataType.class );
    check( "OPTIONS_COMPRESSION", () -> meta.getCompressionType().toUpperCase(), "SNAPPY" );
  }

  protected void checkOrcTypes( String propertyName, IntGetter getter, Class enumType )
    throws KettleException {

    OrcSpec.DataType[] values = OrcSpec.DataType.values();
    ValueMetaInterface valueMeta = new ValueMetaString( "f" );

    for ( OrcSpec.DataType v : values ) {
      injector.setProperty( meta, propertyName, setValue( valueMeta, v.toString() ), "f" );
      assertEquals( v.getId(), getter.get() );
    }

    skipPropertyTest( propertyName );
  }
}
