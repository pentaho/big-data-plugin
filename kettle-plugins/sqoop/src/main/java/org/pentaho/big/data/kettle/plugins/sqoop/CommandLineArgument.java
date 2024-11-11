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


package org.pentaho.big.data.kettle.plugins.sqoop;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Marks a field as a command line argument.
 */
@Documented
@Retention( RetentionPolicy.RUNTIME )
@Target( ElementType.FIELD )
public @interface CommandLineArgument {
  /**
   * @return the name of the command line argument (full name), e.g. --table
   */
  String name();

  /**
   * Optional String to be used when displaying this field in a list.
   * 
   * @return the friendly display name to be shown to a user instead of the {@link #name()}
   */
  String displayName() default "";

  /**
   * @return description of the command line argument
   */
  String description() default "";

  /**
   * Arguments either have values to be included or represent a boolean setting/flag. This is to denote a flag
   * 
   * @return true if this argument represents a flag or switch.
   */
  boolean flag() default false;

  /**
   * Arguments could be prefixed different in a different way (double dash by default)
   * @return prefix to be used with the argument
   */
  String prefix() default "--";

  /**
   * Some arguments have to follow a particular precedence
   * @return sort order for the argument
   */
  int order() default 100;
}
