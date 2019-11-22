/*!
 * Copyright 2019 Hitachi Vantara. All rights reserved.
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

/**
 * The Data Service
 *
 * The Data Service, a collection of endpoints used by the application
 *
 * @module services/data.service
 * @property {String} name The name of the module.
 */
define(
  [],
  function () {
    "use strict";

    var factoryArray = ["helperService", factory];
    var module = {
      name: "dataService",
      factory: factoryArray
    };

    return module;

    /**
     * The dataService factory
     *
     * @param {Object} $http - The $http angular helper service
     *
     * @return {Object} The dataService api
     */
    function factory(helperService) {
      var baseUrl = "/cxf/hadoop-cluster";
      return {
        help: help,
        importNamedCluster: importNamedCluster,
        createNamedCluster: createNamedCluster,
        editNamedCluster: editNamedCluster,
        duplicateNamedCluster: duplicateNamedCluster,
        getNamedCluster: getNamedCluster,
        getShimIdentifiers: getShimIdentifiers,
        runTests: runTests,
        installDriver: installDriver,
        getSecure: getSecure
      };

      function help() {
        return helperService.httpGet([baseUrl, "help"].join("/"));
      }

      function createNamedCluster(data) {
        return helperService.httpPost([baseUrl, "createNamedCluster"].join("/"), data);
      }

      function editNamedCluster(data) {
        return helperService.httpPost([baseUrl, "editNamedCluster"].join("/"), data);
      }

      function duplicateNamedCluster(data) {
        return helperService.httpPost([baseUrl, "duplicateNamedCluster"].join("/"), data);
      }

      function importNamedCluster(data) {
        return helperService.httpPost([baseUrl, "importNamedCluster"].join("/"), data);
      }

      function getNamedCluster(name) {
        return helperService.httpGet([baseUrl, "getNamedCluster"].join("/") + "?namedCluster=" + encodeURIComponent(name));
      }

      function getShimIdentifiers() {
        return helperService.httpGet([baseUrl, "getShimIdentifiers"].join("/"));
      }

      function runTests(name) {
        return helperService.httpGet([baseUrl, "runTests"].join("/") + "?namedCluster=" + encodeURIComponent(name));
      }
      function installDriver(source) {
        return helperService.httpGet([baseUrl, "installDriver"].join("/") + "?source=" + source);
      }
      function getSecure() {
        return helperService.httpGet([baseUrl, "getSecure"].join("/"));
      }
    }
  });
