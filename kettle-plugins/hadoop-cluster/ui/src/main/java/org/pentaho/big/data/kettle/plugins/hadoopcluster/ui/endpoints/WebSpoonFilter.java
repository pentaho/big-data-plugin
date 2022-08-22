/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2022 by Hitachi Vantara : http://www.pentaho.com
 * Copyright (C) 2016-2022 by Hitachi America, Ltd., R&D : http://www.hitachi-america.us/rd/
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

package org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.endpoints;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.Provider;
import org.pentaho.di.core.Const;

@Provider
public class WebSpoonFilter implements ContainerRequestFilter {

  //  Fix for karaf wiring exception in Pentaho Server caused by DTE changes related to:
  //  https://hv-eng.atlassian.net/browse/TAP-5183
  //  If this causes future problems in DTE then it will be necessary to reevaluate
  //  where the org.pentaho.di.ui.repo.endpoints.WebSpoonFilter resides. Probably
  //  move it to "core-ui" as "repositories-plugin-core" is not available in karaf at the Pentaho Server
  //  Moved to org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.endpoints as part of no-osgi feature
  //  work when repositories plugin was removed from karaf.  This package was the only other one referencing it
  //  via beans.xml file.

  @Override
  public void filter( ContainerRequestContext requestContext ) throws IOException {
    if ( Const.isRunningOnWebspoonMode() ) {
      MultivaluedMap<String, String> params = requestContext.getUriInfo().getQueryParameters();
      String cid = params.getFirst( "cid" );
      try {
        Class webSpoonUtils = Class.forName( "org.pentaho.di.webspoon.WebSpoonUtils" );
        Method setUISession = webSpoonUtils.getDeclaredMethod( "setUISession", String.class );
        setUISession.invoke( null, cid );
      } catch ( ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e ) {
        e.printStackTrace();
      }
    }
  }
}
