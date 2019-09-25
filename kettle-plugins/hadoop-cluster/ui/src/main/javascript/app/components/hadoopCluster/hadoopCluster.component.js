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
  'text!./hadoopCluster.html',
  'pentaho/i18n-osgi!hadoopCluster.messages',
  'css!./hadoopCluster.css'
], function (template, i18n) {

  'use strict';

  var options = {
    bindings: {},
    controllerAs: "vm",
    template: template,
    controller: hadoopClusterController
  };

  hadoopClusterController.$inject = ["$location", "$state", "$q", "$stateParams", "dataService"];

  function hadoopClusterController($location, $state, $q, $stateParams, dataService) {
    var vm = this;
    vm.$onInit = onInit;
    vm.onSelect = onSelect;
    vm.onSelectShim = onSelectShim;
    vm.onSelectShimVersion = onSelectShimVersion;
    vm.validateName = validateName;
    vm.onBrowse = onBrowse;
    vm.checkConfigurationPath = checkConfigurationPath;

    function onInit() {
      vm.data = $stateParams.data ? $stateParams.data : {};

      vm.header = i18n.get('hadoop.cluster.header');
      vm.clusterNameLabel = i18n.get('hadoop.cluster.clusterName.label');
      vm.specifyConfigurationLabel = i18n.get('hadoop.cluster.select.type.label');
      vm.configTypePlaceholder = i18n.get('hadoop.cluster.file.placeholder');
      vm.selectPathButtonLabel = i18n.get('hadoop.cluster.select.file.label');
      vm.importLabel = i18n.get('hadoop.cluster.import.label');
      vm.versionLabel = i18n.get('hadoop.cluster.version.label');

      vm.configTypes = [i18n.get('hadoop.cluster.ccfg.type'), i18n.get('hadoop.cluster.site.xml.type')];
      vm.showClusterAndVersion = false;

      dataService.getShimIdentifiers().then(function (res) {
        vm.shimVersionJson = res.data;
        var shimNames = [];
        for (var i = 0; i < res.data.length; i++) {
          if (!contains(shimNames, res.data[i].vendor)) {
            shimNames.push(res.data[i].vendor);
          }
        }
        vm.shimNames = shimNames;

        if ($stateParams.data) {
          vm.configType = vm.data.model.configType;
          vm.shimName = vm.data.model.shimName;
          vm.shimVersion = vm.data.model.shimVersion;
        } else {
          setDialogTitle(i18n.get('hadoop.cluster.title'));
          vm.data = {
            model: {
              clusterName: "",
              configType: "",
              ccfgFilePath: "",
              hadoopConfigFolderPath: "",
              shimName: "",
              shimVersion: ""
            }
          };
          vm.configType = vm.configTypes[0];
          vm.shimName = vm.shimNames[0];
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

    function onSelect(option) {
      vm.data.model.configType = option;
      if (i18n.get('hadoop.cluster.ccfg.type') === option) {
        vm.configTypePlaceholder = i18n.get('hadoop.cluster.file.placeholder');
        vm.selectPathButtonLabel = i18n.get('hadoop.cluster.select.file.label');
        vm.data.model.currentPath = vm.data.model.ccfgFilePath;
        vm.showClusterAndVersion = false;
      } else if (i18n.get('hadoop.cluster.site.xml.type') === option) {
        vm.configTypePlaceholder = i18n.get('hadoop.cluster.folder.placeholder');
        vm.selectPathButtonLabel = i18n.get('hadoop.cluster.select.folder.label');
        vm.data.model.currentPath = vm.data.model.hadoopConfigFolderPath;
        vm.showClusterAndVersion = true;
      }
    }

    function onSelectShim(option) {
      vm.data.model.shimName = option;
      var versions = [];
      for (var i = 0; i < vm.shimVersionJson.length; i++) {
        if (vm.shimVersionJson[i].vendor === option) {
          versions.push(vm.shimVersionJson[i].version);
        }
      }
      vm.shimVersions = versions;
      vm.shimVersion = vm.shimVersions[0];
    }

    function onSelectShimVersion(option) {
      vm.data.model.shimVersion = option;
    }

    function onBrowse() {
      try {
        var path;
        if (i18n.get('hadoop.cluster.site.xml.type') === vm.data.model.configType) {
          path = browse("folder", vm.data.model.hadoopConfigFolderPath);
          if (path) {
            vm.data.model.hadoopConfigFolderPath = path;
            vm.data.model.currentPath = path;
          }
        } else {
          path = browse("file", vm.data.model.ccfgFilePath);
          if (path) {
            vm.data.model.ccfgFilePath = path;
            vm.data.model.currentPath = path;
          }
        }
      } catch (e) {
        vm.data.model.ccfgFilePath = "/";
        vm.data.model.hadoopConfigFolderPath = "/";
        vm.data.model.currentPath = "/";
      }
    }

    function checkConfigurationPath() {
      //Sync the paths for the different types with the current path
      if (i18n.get('hadoop.cluster.site.xml.type') === vm.data.model.configType) {
        vm.data.model.hadoopConfigFolderPath = vm.data.model.currentPath;
      } else {
        vm.data.model.ccfgFilePath = vm.data.model.currentPath;
      }
      //TODO: validation
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
            return !vm.data.model || !vm.data.model.clusterName || !vm.data.model.configType || !vm.data.model.currentPath;
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
    name: "hadoopCluster",
    options: options
  };

});
