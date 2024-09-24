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
 * The File Service
 *
 * A service used to store files in between states so that the files can be uploaded at the same time as the data model
 *
 * Note this service exists because the UI-router is not compatible with files and loses file data between states
 *
 * @module services/file.service
 */
define(
  [],
  function () {
    "use strict";

    var factoryArray = [factory];
    var module = {
      name: "fileService",
      factory: factoryArray
    };

    return module;

    /**
     * The fileService factory
     */
    function factory() {

      var filesList;
      var keytabImpFile;
      var keytabAuthFile;

      return {
        setFiles: setFiles,
        getFiles: getFiles,
        setKeytabImpFile: setKeytabImpFile,
        getKeytabImpFile: getKeytabImpFile,
        setKeytabAuthFile: setKeytabAuthFile,
        getKeytabAuthFile: getKeytabAuthFile
      };

      function setFiles(files) {
        filesList = files;
      }

      function getFiles() {
        return filesList;
      }

      function setKeytabImpFile(file) {
        keytabImpFile = file;
      }

      function getKeytabImpFile() {
        return keytabImpFile;
      }

      function setKeytabAuthFile(file) {
        keytabAuthFile = file;
      }

      function getKeytabAuthFile() {
        return keytabAuthFile;
      }
    }
  });
