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

package org.pentaho.big.data.kettle.plugins.hive;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.pentaho.database.IDatabaseDialect;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Created by bryan on 5/6/16.
 */
public class Activator implements BundleActivator {
  private static final String I_DATABASE_DIALECT_CANONICAL_NAME = IDatabaseDialect.class.getCanonicalName();

  private final List<ServiceRegistration> serviceRegistrations = new ArrayList<>();

  private final List<Supplier<IDatabaseDialect>> databaseDialectSuppliers = Collections.unmodifiableList( Arrays
    .asList( Hive2DatabaseDialect::new, ImpalaDatabaseDialect::new,
      ImpalaSimbaDatabaseDialect::new, SparkSimbaDatabaseDialect::new ) );

  @Override public void start( BundleContext context ) throws Exception {
    serviceRegistrations.addAll( databaseDialectSuppliers.stream()
      .map( supplier -> (ServiceRegistration) context
        .registerService( I_DATABASE_DIALECT_CANONICAL_NAME, supplier.get(), null ) )
      .collect( Collectors.toList() ) );
  }

  @Override public void stop( BundleContext context ) throws Exception {
    serviceRegistrations.forEach( ServiceRegistration::unregister );
    serviceRegistrations.clear();
  }
}
