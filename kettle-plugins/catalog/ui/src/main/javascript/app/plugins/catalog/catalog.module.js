/*!
 * Copyright 2020 Hitachi Vantara. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

define([
  "angular",
  "./catalog.config",
  "./components/step1/step1.component",
  "pentaho/i18n-osgi!catalog.messages"
], function(angular, config, step1Component, i18n) {
  "use strict";

  var module = {
    name: "catalog-plugin",
    scheme: "catalog",
    label: "Catalog",
    summary: [{
      title: i18n.get('Catalog.Label.ConnectionDetails'),
      editLink: "catalogstep1",
      mapping: {
        "url": i18n.get('Catalog.Label.Url'),
        "username": i18n.get('Catalog.Label.Username'),
        "password": i18n.get('Catalog.Label.Password')
      }
    }]
  };
  activate();

  return module;

  /**
   * Creates angular module with dependencies.
   *
   * @private
   */
  function activate() {
    angular.module(module.name, [])
      .component(step1Component.name, step1Component.options)
      .config(config);
  }
});
