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

import org.pentaho.di.trans.TransConfiguration;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by ccaspanello on 8/29/2016.
 */
public class MapReduceTransformations {

  private Optional<TransConfiguration> combiner;
  private Optional<TransConfiguration> mapper;
  private Optional<TransConfiguration> reducer;

  public MapReduceTransformations() {
    this.combiner = Optional.empty();
    this.mapper = Optional.empty();
    this.reducer = Optional.empty();
  }

  //<editor-fold desc="Getters & Setters">
  public Optional<TransConfiguration> getCombiner() {
    return combiner;
  }

  public void setCombiner( Optional<TransConfiguration> combiner ) {
    this.combiner = combiner;
  }

  public Optional<TransConfiguration> getMapper() {
    return mapper;
  }

  public void setMapper( Optional<TransConfiguration> mapper ) {
    this.mapper = mapper;
  }

  public Optional<TransConfiguration> getReducer() {
    return reducer;
  }

  public void setReducer( Optional<TransConfiguration> reducer ) {
    this.reducer = reducer;
  }
  //</editor-fold>

  public List<TransConfiguration> presentTransformations() {
    return Stream.of( combiner, mapper, reducer ).filter( Optional::isPresent ).map( Optional::get )
      .collect( Collectors.toList() );
  }
}
