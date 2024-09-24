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

    var factoryArray = ["$http", "fileService", factory];
    var module = {
      name: "helperService",
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
    function factory($http, fileService) {
      return {
        httpGet: httpGet,
        httpPost: httpPost,
        httpPostMultipart: httpPostMultipart,
        httpPut: httpPut,
        httpDelete: httpDelete
      };

      /**
       * Wraps the http angular service to provide sensible defaults
       *
       * @param {String} url - the url
       * @return {Promise} - a promise to be resolved as soon as we get confirmation from the server.
       * @private
       */
      function httpGet(url) {
        return _wrapHttp("GET", url);
      }

      /**
       * Wraps the http angular service to provide sensible defaults
       *
       * @param {String} url - the url
       * @param {String} data - the post data
       * @return {Promise} - a promise to be resolved as soon as we get confirmation from the server.
       * @private
       */
      function httpPost(url, data) {
        return _wrapHttp("POST", url, data);
      }

      /**
       * Wraps the http angular service for multipart post
       *
       * @param {String} url - the url
       * @param {String} data - the post data
       * @return {Promise} - a promise to be resolved as soon as we get confirmation from the server.
       */
      function httpPostMultipart(url, data) {
        var fd = new FormData();

        //Append the files to the form data
        var files = fileService.getFiles();
        if (files) {
          for (var i = 0; i < files.length; i++) {
            fd.append( "file-"+files[i].name, files[i] );
            if ( 'lastModified' in files[i] ) {
              fd.append( "mod-"+files[i].name, files[i].lastModified );
            } else if ( 'lastModifiedDate' in files[i]) {
              fd.append( "mod-"+files[i].name, Date.parse( files[i].lastModifiedDate ) );
            }
          }
          fileService.setFiles(null);
        }

        var keytabAuthFile = fileService.getKeytabAuthFile();
        if (keytabAuthFile) {
          fd.append("keytabAuthFile", keytabAuthFile);
          fileService.setKeytabAuthFile(null);
        }

        var keytabImpFile = fileService.getKeytabImpFile();
        if (keytabImpFile) {
          fd.append("keytabImpFile", keytabImpFile);
          fileService.setKeytabImpFile(null);
        }

        //Append the data model to the form data
        if (data) {
          fd.append("data", JSON.stringify(data.model));
        }

        //Even though content type is multipart we set to undefined and angular handles it
        return $http.post(_cacheBust(url), fd, {
            transformRequest: angular.identity,
            headers: {'Content-Type': undefined, Accept: "application/json"}
          }
        );
      }

      /**
       * Wraps the http angular service to provide sensible defaults
       *
       * @param {String} url - the url
       * @param {String} data - the put data
       * @return {Promise} - a promise to be resolved as soon as we get confirmation from the server.
       * @private
       */
      function httpPut(url, data) {
        return _wrapHttp("PUT", url, data);
      }

      /**
       * Wraps the http angular service to provide sensible defaults
       *
       * @param {String} url - the url
       * @return {Promise} - a promise to be resolved as soon as we get confirmation from the server.
       * @private
       */
      function httpDelete(url) {
        return _wrapHttp("DELETE", url);
      }

      /**
       * Wraps the http angular service to provide sensible defaults
       *
       * @param {String} method - the http method to use
       * @param {String} url - the url
       * @param {String} data - the data to send to the server
       * @return {Promise} - a promise to be resolved as soon as we get confirmation from the server.
       * @private
       */
      function _wrapHttp(method, url, data) {
        var options = {
          method: method,
          url: _cacheBust(url),
          headers: {
            Accept: "application/json"
          }
        };
        if (data !== null) {
          options.data = data;
        }
        return $http(options);
      }

      /**
       * Eliminates cache issues
       * @param {String} url - url string
       * @return {*} - url
       * @private
       */
      function _cacheBust(url) {
        var value = Math.round(new Date().getTime() / 1000) + Math.random();
        if (url.indexOf("?") !== -1) {
          url += "&v=" + value;
        } else {
          url += "?v=" + value;
        }
        if (typeof getConnectionId == 'function') {
          var cid = getConnectionId();
          url += "&cid=" + cid;
        }
        return url;
      }
    }
  });
