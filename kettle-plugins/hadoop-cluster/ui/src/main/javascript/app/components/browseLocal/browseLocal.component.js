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
  'text!./browseLocal.html',
  'pentaho/i18n-osgi!hadoopCluster.messages',
  'css!./browseLocal.css'
], function (template, i18n) {

  'use strict';

  var options = {
    bindings: {
      type: "<", //folder or file
      label: "<",
      model: "=",
      browseLabel: "<?", //optional - has defaults
      placeholder: "<?", // optional - has defaults
      validation: "&?" //optional
    },
    controllerAs: "vm",
    template: template,
    controller: browseLocalController
  };

  browseLocalController.$inject = ["$document", "$scope"];

  function browseLocalController($document, $scope) {
    var vm = this;
    vm.$onInit = onInit;
    vm.onBrowse = onBrowse;

    function onInit() {
      if (vm.type === "folder") {
        if (!vm.placeholder) {
          vm.placeholder = i18n.get('browse.local.folder.placeholder');
        }
        if (!vm.browseLabel) {
          vm.browseLabel = i18n.get('browse.local.folder.button');
        }
      } else {
        if (!vm.placeholder) {
          vm.placeholder = i18n.get('browse.local.file.placeholder');
        }
        if (!vm.browseLabel) {
          vm.browseLabel = i18n.get('browse.local.file.button');
        }
      }
    }

    function onBrowse() {
      try {
        var path;
        if (vm.type === "folder") {
          path = browse("folder", vm.model);
        } else {
          path = browse("file", vm.model);
        }
        if (path) {
          vm.model = path;
        }
      } catch (e) {
        vm.model = "/";
      }
    }
  }

  return {
    name: "browseLocal",
    options: options
  };

});
