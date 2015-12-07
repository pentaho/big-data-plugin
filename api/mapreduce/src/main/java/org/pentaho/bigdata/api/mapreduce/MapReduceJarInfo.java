/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

package org.pentaho.bigdata.api.mapreduce;

import java.util.List;

/**
 * Created by bryan on 12/4/15.
 */
public interface MapReduceJarInfo {
  /**
   * Returns a list of classes in the jar that have a method with the signature: public static void main(String[] args)
   *
   * @return a list of classes in the jar that have a method with the signature: public static void main(String[] args)
   */
  List<String> getClassesWithMain();

  /**
   * Returns the main class as listed in the manifest of the jar
   *
   * @return the main class as listed in the manifest of the jar
   */
  String getMainClass();
}
