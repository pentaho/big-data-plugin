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

package org.pentaho.runtime.test;

import org.pentaho.runtime.test.result.RuntimeTestResultSummary;

import java.util.Set;

/**
 * Created by bryan on 8/11/15.
 */
public interface RuntimeTest {
  boolean accepts( Object objectUnderTest );

  String getModule();

  String getId();

  String getName();

  boolean isConfigInitTest();

  Set<String> getDependencies();

  RuntimeTestResultSummary runTest( Object objectUnderTest );
}
