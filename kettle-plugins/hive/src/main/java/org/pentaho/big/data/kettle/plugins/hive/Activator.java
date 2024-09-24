/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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
