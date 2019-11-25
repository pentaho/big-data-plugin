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
 * The Clusters Service
 *
 * The Clusters Service, a collection of endpoints used by the application
 *
 * @module services/clusters.service
 * @property {String} name The name of the module.
 */
define(
    ['css!../../css/hadoop-clusters.css'], function () {
      "use strict";

      var factoryArray = ["helperService", "$http", "$q", factory];
      var module = {
        name: "clusters",
        factory: factoryArray
      };

      return module;

      /**
       * The dataService factory
       *
       * @param {Object} helperService
       * @param {Object} $http - The $http angular helper service
       * @param {Object} $q
       *
       * @return {Object} The dataService api
       */
      function factory(helperService, $http, $q) {
        var baseUrl = "/cxf/browser-new";
        return {
          provider: "clusters",
          order: 3,
          root: "Hadoop Clusters",
          matchPath: matchPath,
          selectFolder: selectFolder,
          getBreadcrumbPath: getBreadcrumbPath,
          getPath: getPath,
          getFilesByPath: getFilesByPath,
          createFolder: createFolder,
          addFolder: addFolder,
          deleteFiles: deleteFiles,
          renameFile: renameFile,
          isCopy: isCopy,
          open: open,
          save: save,
          resolvePath: resolvePath
        };

        function resolvePath(path, properties) {
          return $q(function (resolve, reject) {
            resolve("Hadoop Clusters/" + path.replace("hc://", ''));
          });
        }

        function getPath(file) {
          return file.root ? _getTreePath(file) : file.path;
        }

        function matchPath(path) {
          return (path && path.match(/^hc:\/\//) != null) ? 1 : 0;
        }

        function selectFolder(folder, filters, useCache) {
          return $q(function (resolve, reject) {
            if (folder.path && !folder.loaded) {
              getFiles(folder, filters, useCache).then(function (response) {
                folder.children = response.data;
                folder.loaded = true;
                for (var i = 0; i < folder.children.length; i++) {
                  folder.children[i].provider = folder.provider;
                }
                resolve();
              });
            } else {
              resolve();
            }
          });
        }

        function getBreadcrumbPath(file) {
          return {
            type: this.provider,
            fileType: file.type ? file.type : "folder",
            prefix: _getFilePrefix(file),
            uri: _getFilePath(file),
            path: _getTreePath(file)
          };
        }

        function _getFilePrefix(file) {
          if (file.root) {
            return null;
          }
          return file.path ? file.path.match(/^hc+:\/\//)[0] : null;
        }

        function _getTreePath(folder) {
          if (!folder.path) {
            return folder.root ? folder.root + "/" + folder.name : folder.name;
          }
          return folder.root + "/" + folder.path.replace(/^hc+:\/\//, "");
        }

        function _getFilePath(file) {
          return file.path ? file.path : null;
        }

        /**
         *
         * @param node
         * @param name
         * @returns {*}
         */
        function createFolder(node, name) {
          return $q(function (resolve, reject) {
            if ((node.path && !node.loaded) || (!node.path && !node.loaded)) {
              getFiles(node).then(function (response) {
                node.loaded = true;
                resolve(response.data);
              });
            } else {
              reject();
            }
          });
        }

        function getFilesByPath(path, useCache) {
          return getFiles({path: path, provider: this.provider}, undefined, useCache);
        }

        /**
         * Gets the directory tree for the currently connected repository
         *
         * @return {Promise} - a promise resolved once data is returned
         */
        function getFiles(folder, filters, useCache) {
          return helperService.httpPost([baseUrl, "getFiles"].join("/"), folder);
        }

        function addFolder(folder) {
          return helperService.httpPut([baseUrl, "add"].join("/"), folder);
        }

        function deleteFiles(files) {
          return helperService.httpPost([baseUrl, "delete"].join("/"), files);
        }

        function renameFile(file, newPath) {
          return $q(function (resolve, reject) {
            helperService.httpPost([baseUrl, "rename"].join("/") + "?newPath=" + newPath, file).then(function (response) {
              file.path = newPath;
            });
          });
        }

        function isCopy(from, to) {
          return from.provider !== to.provider;
        }

        function open(file) {
          select(JSON.stringify({
            name: file.name,
            path: file.path,
            parent: file.parent,
            provider: file.provider
          }));
        }

        function save(filename, folder) {
          select(JSON.stringify({
            name: filename,
            path: folder.path,
            parent: folder.parent,
            provider: folder.provider
          }));

          return $q.resolve();
        }
      }
    });
