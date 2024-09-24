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
