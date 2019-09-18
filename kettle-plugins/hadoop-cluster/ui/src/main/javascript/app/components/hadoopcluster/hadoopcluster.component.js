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
  'text!./hadoopcluster.html',
  'pentaho/i18n-osgi!hadoop-cluster.messages',
  'css!./hadoopcluster.css'
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
    vm.resetErrorMsg = resetErrorMsg;
    vm.checkClusterName = checkClusterName;
    vm.onBrowse = onBrowse;
    vm.checkConfigurationPath = checkConfigurationPath;
    vm.configurationType = null;
    var loaded = false;
    vm.selectConfigPathButtonLabel = "";


    function onInit() {
      vm.data = $stateParams.data ? $stateParams.data : {};
      vm.clusterNameLabel = i18n.get('cluster.hadoop.clusterName.label');
      vm.specifyConfigurationLabel = i18n.get('cluster.hadoop.specify.configuration.label');
      vm.title = i18n.get('cluster.hadoop.new.header');
      vm.configurationTypes = [i18n.get('cluster.hadoop.import.ccfg'), i18n.get('cluster.hadoop.provide.site.xml')];
      vm.configurationType = vm.configurationTypes[0];
      vm.configurationPathPlaceholder = i18n.get('cluster.hadoop.no.ccfg.selected.placeholder');
      vm.selectConfigPathButtonLabel = i18n.get('cluster.hadoop.selectCcfgFileButtonLabel');
      vm.next = "/";

      vm.importLabel = i18n.get('cluster.hadoop.import.label');
      vm.versionLabel = i18n.get('cluster.hadoop.version.label');
      dataService.getShimIdentifiers().then(function (res) {
        vm.shimVersionJson = res.data;
        var shimNames = [];
        for (var i = 0; i < res.data.length; i++) {
          if (!contains(shimNames, res.data[i].vendor)) {
            shimNames.push(res.data[i].vendor);
          }
        }
        vm.shimNames = shimNames;
        vm.shimName = vm.shimNames[0];
      });

      if ($stateParams.data) {
        //TODO: future implementation use ui-router for saving data between screens

      } else {

        setDialogTitle(i18n.get('cluster.hadoop.title'));

        vm.data = {
          model: {
            clusterName: "",
            configurationType: "",
            ccfgFilePath: "",
            hadoopConfigFolderPath: "",
            shimName: "",
            shimVersion: ""
          }
        };
      }


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

    function resetErrorMsg() {
      if (!vm.data.isSaved) {
        vm.data.state = "new";
        vm.title = i18n.get('cluster.hadoop.new.header');
        setDialogTitle(i18n.get('cluster.hadoop.title'));
      }
      vm.errorMessage = null;
    }

    function onSelect(option) {
      vm.data.model.configurationType = option;

      if (i18n.get('cluster.hadoop.import.ccfg') === option) {
        vm.configurationPathPlaceholder = i18n.get('cluster.hadoop.no.ccfg.selected.placeholder');
        vm.selectConfigPathButtonLabel = i18n.get('cluster.hadoop.selectCcfgFileButtonLabel');
        vm.data.model.currentPath = vm.data.model.ccfgFilePath;
      } else if (i18n.get('cluster.hadoop.provide.site.xml') === option) {
        vm.configurationPathPlaceholder = i18n.get('cluster.hadoop.no.config.placeholder');
        vm.selectConfigPathButtonLabel = i18n.get('cluster.hadoop.config.folder.button.label');
        vm.data.model.currentPath = vm.data.model.hadoopConfigFolderPath;
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
        if (i18n.get('cluster.hadoop.provide.site.xml') === vm.data.model.configurationType) {
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

    function checkClusterName() {
      //TODO: implement
    }

    function checkConfigurationPath() {
      //Sync the paths for the different types with the current path
      if (i18n.get('cluster.hadoop.provide.site.xml') === vm.data.model.configurationType) {
        vm.data.model.hadoopConfigFolderPath = vm.data.model.currentPath;
      } else {
        vm.data.model.ccfgFilePath = vm.data.model.currentPath;
      }
      //TODO: validation
    }

    function validateName() {
      //TODO: implement
      return $q(function (resolve) {
        return resolve(true)
      });
    }

    function setDialogTitle(title) {
      if (loaded === true) {
        try {
          setTitle(title);
        } catch (e) {
          console.log(title);
        }
      }
    }

    function getButtons() {
      return [
        {
          label: i18n.get('cluster.controls.nextLabel'),
          class: "primary",
          isDisabled: function () {
            return !vm.data.model || !vm.data.model.clusterName || !vm.data.model.configurationType || !vm.data.model.currentPath;
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
          label: i18n.get('cluster.controls.cancelLabel'),
          class: "primary",
          position: "right",
          onClick: function () {
            close();
          }
        }];
    }
  }

  return {
    name: "hadoopcluster",
    options: options
  };

});
