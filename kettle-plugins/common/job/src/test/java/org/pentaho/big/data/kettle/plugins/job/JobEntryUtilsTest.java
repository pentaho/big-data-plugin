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
package org.pentaho.big.data.kettle.plugins.job;

import org.junit.Test;

import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import static org.junit.Assert.*;

/**
 * User: RFellows Date: 6/7/12
 */
public class JobEntryUtilsTest {

    @Test
    public void asBoolean() {
        VariableSpace variableSpace = new Variables();

        assertFalse( JobEntryUtils.asBoolean( "not-true", variableSpace ) );
        assertFalse( JobEntryUtils.asBoolean( Boolean.FALSE.toString(), variableSpace ) );
        assertTrue( JobEntryUtils.asBoolean( Boolean.TRUE.toString(), variableSpace ) );

        // No variable set, should attempt convert ${booleanValue} as is
        assertFalse( JobEntryUtils.asBoolean( "${booleanValue}", variableSpace ) );

        variableSpace.setVariable( "booleanValue", Boolean.TRUE.toString() );
        assertTrue( JobEntryUtils.asBoolean( "${booleanValue}", variableSpace ) );

        variableSpace.setVariable( "booleanValue", Boolean.FALSE.toString() );
        assertFalse( JobEntryUtils.asBoolean( "${booleanValue}", variableSpace ) );
    }

    @Test
    public void asLong() {
        VariableSpace variableSpace = new Variables();

        assertNull( JobEntryUtils.asLong( null, variableSpace ) );
        assertEquals( Long.valueOf( "10", 10 ), JobEntryUtils.asLong( "10", variableSpace ) );

        variableSpace.setVariable( "long", "150" );
        assertEquals( Long.valueOf( "150", 10 ), JobEntryUtils.asLong( "${long}", variableSpace ) );

        try {
            JobEntryUtils.asLong( "NaN", variableSpace );
            fail( "expected number format exception" );
        } catch ( NumberFormatException ex ) {
            // we're good
        }
    }
}
