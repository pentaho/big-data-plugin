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
