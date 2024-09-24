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
      var baseUrl = "../../cxf/hadoop-cluster";
      return {
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

      function createNamedCluster(data) {
        return helperService.httpPostMultipart([baseUrl, "createNamedCluster"].join("/"), data);
      }

      function editNamedCluster(data) {
        return helperService.httpPostMultipart([baseUrl, "editNamedCluster"].join("/"), data);
      }

      function duplicateNamedCluster(data) {
        return helperService.httpPostMultipart([baseUrl, "duplicateNamedCluster"].join("/"), data);
      }

      function importNamedCluster(data) {
        return helperService.httpPostMultipart([baseUrl, "importNamedCluster"].join("/"), data);
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
      function installDriver() {
        return helperService.httpPostMultipart([baseUrl, "installDriver"].join("/"));
      }
      function getSecure() {
        return helperService.httpGet([baseUrl, "getSecure"].join("/"));
      }
    }
  });
