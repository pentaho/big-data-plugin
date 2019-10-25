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
  'text!./import.html',
  'pentaho/i18n-osgi!hadoopCluster.messages',
  'css!./import.css'
], function (template, i18n) {

  'use strict';

  var options = {
    bindings: {},
    controllerAs: "vm",
    template: template,
    controller: importController
  };

  importController.$inject = ["$location", "$state", "$q", "$stateParams", "dataService"];

  function importController($location, $state, $q, $stateParams, dataService) {
    var vm = this;
    vm.$onInit = onInit;
    vm.onSelectShim = onSelectShim;
    vm.onSelectShimVersion = onSelectShimVersion;
    vm.validateName = validateName;
    vm.getShimVersions = getShimVersions;

    function onInit() {
      vm.data = $stateParams.data ? $stateParams.data : {};
      vm.browseType = "folder";
      vm.header = i18n.get('import.header');
      vm.clusterNameLabel = i18n.get('hadoop.cluster.name.label');
      vm.importFolderLabel = i18n.get('import.folder.label');
      vm.importLabel = i18n.get('hadoop.cluster.import.label');
      vm.versionLabel = i18n.get('hadoop.cluster.version.label');
      vm.hdfsLabel = i18n.get('hadoop.cluster.hdfs.label');

      setDialogTitle(i18n.get('hadoop.cluster.title'));

      dataService.getShimIdentifiers().then(function (res) {
        vm.shimVersionJson = res.data;
        var shimVendors = [];
        for (var i = 0; i < res.data.length; i++) {
          if (!contains(shimVendors, res.data[i].vendor)) {
            shimVendors.push(res.data[i].vendor);
          }
        }
        vm.shimVendors = shimVendors;

        if ($stateParams.data) {
          vm.shimVendor = vm.data.model.shimVendor;
          vm.shimVersions = getShimVersions(vm.shimVendor);
          vm.shimVersion = vm.data.model.shimVersion;
        } else {
          vm.data = {
            model: {
              name: "",
              importPath: "",
              shimVendor: "",
              shimVersion: "",
              hdfsUsername: "",
              hdfsPassword: ""
            }
          };
          vm.data.created = false;
          vm.data.type = "import";
          vm.shimVendor = vm.shimVendors[0];
        }
      });

      vm.buttons = getButtons();
    }

    function contains(arr, item) {
      for (var i = 0; i < arr.length; i++) {
        if (arr[i] === item) {
          return true;
        }
      }
      return false;
    }

    function onSelectShim(option) {
      vm.data.model.shimVendor = option;
      vm.shimVersions = getShimVersions(option);
      if (!vm.shimVersion || contains(vm.shimVersions, vm.shimVersion) === false) {
        vm.shimVersion = vm.shimVersions[0];
      }
    }

    function getShimVersions(shimVendor) {
      var versions = [];
      for (var i = 0; i < vm.shimVersionJson.length; i++) {
        if (vm.shimVersionJson[i].vendor === shimVendor) {
          versions.push(vm.shimVersionJson[i].version);
        }
      }
      return versions;
    }

    function onSelectShimVersion(option) {
      vm.data.model.shimVersion = option;
    }

    function validateName() {
      //TODO: implement
      return $q(function (resolve) {
        return resolve(true);
      });
    }

    function setDialogTitle(title) {
      try {
        setTitle(title);
      } catch (e) {
        console.log(title);
      }
    }

    function getButtons() {
      return [
        {
          label: i18n.get('controls.next.label'),
          class: "primary",
          isDisabled: function () {
            return !vm.data.model || !vm.data.model.name || !vm.data.model.importPath;
          },
          position: "right",
          onClick: function () {
            validateName().then(function (isValid) {
              if (isValid) {
                $state.go('creating', {data: vm.data, transition: "slideLeft"});
              }
            });
          }
        },
        {
          label: i18n.get('controls.cancel.label'),
          class: "primary",
          position: "right",
          onClick: function () {
            close();
          }
        }];
    }
  }

  return {
    name: "import",
    options: options
  };

});
