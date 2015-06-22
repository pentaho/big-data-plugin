package org.pentaho.di.bigdata;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Documented
@Retention( RetentionPolicy.RUNTIME )
@Target( ElementType.TYPE )
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
