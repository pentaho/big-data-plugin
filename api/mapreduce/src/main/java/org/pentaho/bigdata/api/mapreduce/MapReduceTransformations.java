/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

import org.pentaho.di.trans.TransMeta;

import java.util.Optional;

/**
 * Created by ccaspanello on 8/29/2016.
 */
public class MapReduceTransformations {

  private Optional<TransMeta> combiner;
  private Optional<TransMeta> mapper;
  private Optional<TransMeta> reducer;

  public MapReduceTransformations() {
    this.combiner = Optional.empty();
    this.mapper = Optional.empty();
    this.reducer = Optional.empty();
  }

  //<editor-fold desc="Getters & Setters">
  public Optional<TransMeta> getCombiner() {
    return combiner;
  }

  public void setCombiner( Optional<TransMeta> combiner ) {
    this.combiner = combiner;
  }

  public Optional<TransMeta> getMapper() {
    return mapper;
  }

  public void setMapper( Optional<TransMeta> mapper ) {
    this.mapper = mapper;
  }

  public Optional<TransMeta> getReducer() {
    return reducer;
  }

  public void setReducer( Optional<TransMeta> reducer ) {
    this.reducer = reducer;
  }
  //</editor-fold>
}
