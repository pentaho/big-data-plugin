/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/
package org.pentaho.big.data.kettle.plugins.job;

import org.pentaho.di.core.variables.VariableSpace;

import java.util.Map;

/**
 * User: RFellows Date: 6/7/12
 */
public class JobEntryUtils {

    /**
     * @return {@code true} if {@link Boolean#parseBoolean(String)} returns {@code true} for
     *         {@link #isBlockingExecution()}
     */
    /**
     * Determine if the string equates to {@link Boolean#TRUE} after performing a variable substitution.
     *
     * @param s
     *          String-encoded boolean value or variable expression
     * @param variableSpace
     *          Context for variables so we can substitute {@code s}
     * @return the value returned by {@link Boolean#parseBoolean(String) Boolean.parseBoolean(s)} after substitution
     */
    public static boolean asBoolean( String s, VariableSpace variableSpace ) {
        String value = variableSpace.environmentSubstitute( s );
        return Boolean.parseBoolean( value );
    }

    /**
     * Parse the string as a {@link Long} after variable substitution.
     *
     * @param s
     *          String-encoded {@link Long} value or variable expression that should resolve to a {@link Long} value
     * @param variableSpace
     *          Context for variables so we can substitute {@code s}
     * @return the value returned by {@link Long#parseLong(String, int) Long.parseLong(s, 10)} after substitution
     */
    public static Long asLong( String s, VariableSpace variableSpace ) {
        String value = variableSpace.environmentSubstitute( s );
        return value == null ? null : Long.valueOf( value, 10 );
    }
}
