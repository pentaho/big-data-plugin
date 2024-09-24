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

define([
  'angular'
], function (angular) {

  'use strict';

  function multiFileInput($parse) {
    return {
      restrict: 'A',
      link: function (scope, element, attr) {
        element.bind('change', function () {
          var files = [];

          //Get the new files
          var filesToAdd = element[0].files;

          //Convert to array because file list is readonly
          if (filesToAdd instanceof FileList) {
            for (var i = 0; i < filesToAdd.length; i++) {
              files.push(filesToAdd[i]);
            }
          }

          //Get the existing files from the model
          var existingFiles = $parse(attr.multiFileInput)(scope);

          //Concat the new files onto the existing array of files
          if (existingFiles && existingFiles.length > 0) {
            files = existingFiles.concat(files);
          }

          //Set the model to the concatenated file list
          $parse(attr.multiFileInput).assign(scope, files);

          //reset the element so that we get a change event for every selection of files
          element[0].value = '';

          scope.$apply();
        });
      }
    };
  }

  return {
    name: "multiFileInput",
    options: ["$parse", multiFileInput]
  };
});
