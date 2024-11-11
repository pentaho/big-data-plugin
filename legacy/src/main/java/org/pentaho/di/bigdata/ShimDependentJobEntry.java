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


package org.pentaho.di.bigdata;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Documented
@Retention( RetentionPolicy.RUNTIME )
@Target( ElementType.TYPE )
/**
 * @deprecated Use OSGi plugins instead to access shim functionality
 */
@Deprecated()
public @interface ShimDependentJobEntry {
  String id();

  String name() default "";

  String description() default "";

  String image();

  String version() default "";

  int category() default -1;

  String categoryDescription() default "";

  String i18nPackageName() default "";

  String documentationUrl() default "";

  String casesUrl() default "";

  String forumUrl() default "";
}
