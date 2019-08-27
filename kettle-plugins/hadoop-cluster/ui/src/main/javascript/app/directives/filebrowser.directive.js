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

  function filebrowser() {
    return {
      restrict: 'A',
      template: '<input id="browserFileInput" (change)="fileChangeEvent($event)" type="file" style="display: none;" />' +
          '<ng-transclude></ng-transclude>',
      transclude: true,
      link: function (scope, element) {
         element.bind('click', function (e) {
           angular.element('#browserFileInput')[0].click();
         });
         element.bind('change', function(e) {
           window.alert(fileInput.target.files[0]);
           angular.element('#browserFileInput')[0].files[0];
         });
      }
    }
  }

  // function fileChangeEvent(fileInput) {
  //   if(fileInput.target.files && fileInput.target.files[0]) {
  //     window.alert(fileInput.target.files[0]);
  //   }
  //
  // }

  return {
    name: "filebrowser",
    options: [filebrowser]
  }
});
