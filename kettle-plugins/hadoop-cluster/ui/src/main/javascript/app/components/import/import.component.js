/*!
 * Copyright 2019-2020 Hitachi Vantara. All rights reserved.
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

  importController.$inject = ["$location", "$state", "$q", "$stateParams", "dataService", "fileService"];

  function importController($location, $state, $q, $stateParams, dataService, fileService) {
    var vm = this;
    vm.$onInit = onInit;
    vm.onSelectShim = onSelectShim;
    vm.onSelectShimVersion = onSelectShimVersion;
    vm.getShimVersions = getShimVersions;

    var modalDialogElement = angular.element("#modalDialog");
    var modalOverlayElement = angular.element("#modalOverlay");

    var connectedToRepo = ($stateParams.data && $stateParams.data.connectedToRepo) || $location.search().connectedToRepo === 'true';

    var notAllowedChars = ['\\\\', '/', '*', ':', '?', '"', '<', '>', '|', '%', '+', '#'];
    var notAllowedGlobalRegex = new RegExp("[" + notAllowedChars.join('') + "]", "g");
    notAllowedChars[0] = '\\'; //remove extra escape for the backslash char in the array

    function onInit() {
      vm.data = $stateParams.data ? $stateParams.data : {};
      vm.header = i18n.get('import.header');
      vm.repositoryNotification = i18n.get('repositoryNotification.header');
      vm.clusterNameLabel = i18n.get('hadoop.cluster.name.label');
      vm.importFolderLabel = i18n.get('import.folder.label');
      vm.importLabel = i18n.get('hadoop.cluster.import.label');
      vm.versionLabel = i18n.get('hadoop.cluster.version.label');
      vm.hdfsLabel = i18n.get('hadoop.cluster.hdfs.label');
      vm.dialogTitle = i18n.get('hadoop.cluster.overwrite.title');
      vm.dialogMessage = i18n.get('hadoop.cluster.overwrite.message');
      vm.helpLink = i18n.get('import.help');

      vm.onNameChange = onNameChange;
      vm.onNameKeyDown = onNameKeyDown;

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
          vm.siteFiles = fileService.getFiles();
        } else {
          vm.data = {
            model: {
              name: "",
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
        vm.data.connectedToRepo = connectedToRepo;
      });

      vm.buttons = getButtons();
      vm.overwriteDialogButtons = getOverwriteDialogButtons();
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

    function checkDuplicateName(name) {
      return $q(function (resolve, reject) {
        dataService.getNamedCluster(name).then(
          function (res) {
            //if name is returned it is a duplicate
            if (res.data.name && res.data.name.length !== 0) {
              reject();
            } else {
              resolve();
            }
          });
      });
    }

    function setDialogTitle(title) {
      try {
        setTitle(title);
      } catch (e) {
        console.log(title);
      }
    }

    // Prevent pressing characters not allowed in cluster name
    function onNameKeyDown($event) {
      return !contains(notAllowedChars, $event.key);
    }

    // Replace not allowed characters if they got into the name field somehow
    function onNameChange() {
      vm.data.model.name = cleanseName(vm.data.model.name);
    }

    function cleanseName(s) {
      return s.replace(notAllowedGlobalRegex, '');
    }

    function next() {
      vm.data.connectedToRepo = connectedToRepo;
      // should not be possible to get to this point w/o a cleansed name, but just to be safe
      vm.data.model.name = cleanseName(vm.data.model.name);
      var promise = checkDuplicateName(vm.data.model.name);
      promise.then(
        create,
        function () {
          displayOverwriteDialog(true);
        }
      );
    }

    function create() {
      dataService.getSecure().then(function (res) {

        //UI-router doesn't work to pass files between states, use fileservice to store the file(s), they are later
        //retrieved by the helperService before passing the request to the server.
        fileService.setFiles(vm.siteFiles);

        if (res.data.secureEnabled === "true") {
          $state.go('security', {data: vm.data, transition: "slideLeft"});
        } else {
          $state.go('creating', {data: vm.data, transition: "slideLeft"});
        }
      });
    }

    function getButtons() {
      return [
        {
          label: i18n.get('controls.next.label'),
          class: "primary",
          isDisabled: function () {
            return !vm.data.model || !vm.data.model.name || !vm.siteFiles;
          },
          position: "right",
          onClick: next
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

    function getOverwriteDialogButtons() {
      return [
        {
          label: i18n.get('hadoop.cluster.overwrite.cancel'),
          class: "primary",
          position: "right",
          onClick: function () {
            displayOverwriteDialog(false);
          }
        },
        {
          label: i18n.get('hadoop.cluster.overwrite.yes'),
          class: "primary",
          position: "right",
          onClick: create
        }];
    }

    function displayOverwriteDialog(show) {
      var display = "none";
      if (show === true) {
        display = "block";
      }
      modalDialogElement.css("display", display);
      modalOverlayElement.css("display", display);
    }

  }

  return {
    name: "import",
    options: options
  };

});
