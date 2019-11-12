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
  'text!./newEdit.html',
  'pentaho/i18n-osgi!hadoopCluster.messages',
  'css!./newEdit.css'
], function (template, i18n) {

  'use strict';

  var options = {
    bindings: {},
    controllerAs: "vm",
    template: template,
    controller: newEditController
  };

  newEditController.$inject = ["$location", "$state", "$q", "$stateParams", "dataService"];

  function newEditController($location, $state, $q, $stateParams, dataService) {
    var vm = this;
    vm.$onInit = onInit;
    vm.onSelectShim = onSelectShim;
    vm.onSelectShimVersion = onSelectShimVersion;
    vm.getShimVersions = getShimVersions;

    var modalDialogElement = angular.element("#modalDialog");
    var modalOverlayElement = angular.element("#modalOverlay");

    vm.variableImage = "img/variable.svg";

    function onInit() {
      vm.data = $stateParams.data ? $stateParams.data : {};

      vm.clusterNameLabel = i18n.get('hadoop.cluster.name.label');
      vm.importLabel = i18n.get('hadoop.cluster.import.label');
      vm.versionLabel = i18n.get('hadoop.cluster.version.label');
      vm.browseType = "folder";
      vm.importFolderLabel = i18n.get('new.edit.folder.label.optional');
      vm.hdfsLabel = i18n.get('hadoop.cluster.hdfs.label');
      vm.hostnameLabel = i18n.get('new.edit.hostname.label');
      vm.portLabel = i18n.get('new.edit.port.label');
      vm.usernameLabel = i18n.get('new.edit.username.label');
      vm.passwordLabel = i18n.get('new.edit.password.label');
      vm.jobTrackerLabel = i18n.get('new.edit.job.tracker.label');
      vm.zooKeeperLabel = i18n.get('new.edit.zoo.keeper.label');
      vm.oozieLabel = i18n.get('new.edit.oozie.label');
      vm.kafkaLabel = i18n.get('new.edit.kafka.label');
      vm.bootstrapServerLabel = i18n.get('new.edit.bootstrap.server.label');
      vm.dialogTitle = i18n.get('hadoop.cluster.overwrite.title');
      vm.dialogMessage = i18n.get('hadoop.cluster.overwrite.message');

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

        var urlNameParameter = $location.search().name;
        if (urlNameParameter) {
          vm.header = i18n.get('edit.header');
          vm.data.type = "edit";
          dataService.getNamedCluster(urlNameParameter).then(function (res) {
            vm.data.model = res.data;
            vm.data.model.oldName = urlNameParameter;
            var duplicateName = $location.search().duplicateName;
            if (duplicateName) {
              vm.data.model.name = duplicateName;
              vm.data.type = "duplicate";
            }
            loadShimDropDowns();
          });
        } else if (vm.data.model && vm.data.model.name) {
          //When an import is created and then edited - it is converted to an edit
          if (vm.data.created === true) {
            vm.header = i18n.get('edit.header');
            vm.data.type = "edit";
            vm.data.created = false;
            dataService.getNamedCluster(vm.data.model.name).then(function (res) {
              vm.data.model = res.data;
              vm.data.model.oldName = vm.data.model.name;
              loadShimDropDowns();
            });
          } else {
            //All the data already exists in the model, so leave it as is
            loadShimDropDowns();
          }
        } else {
          //this is a new state, no name exists in the model or on the URL
          vm.header = i18n.get('new.header');
          vm.data = createHadoopDataModel();
          vm.data.type = "new";
          vm.data.created = false;
          vm.shimVendor = vm.shimVendors[0];
        }
      });

      vm.buttons = getButtons();
      vm.overwriteDialogButtons = getOverwriteDialogButtons();
    }

    function createHadoopDataModel() {
      return {
        model: {
          name: "",
          shimVendor: "",
          shimVersion: "",
          importPath: "",
          hdfsHost: "",
          hdfsPort: "",
          hdfsUsername: "",
          hdfsPassword: "",
          jobTrackerHost: "",
          jobTrackerPort: "",
          zooKeeperHost: "",
          zooKeeperPort: "",
          oozieUrl: "",
          kafkaBootstrapServers: "",
          securityType: "None",
          kerberosSubType: "Password",
          kerberosAuthenticationUsername: "",
          kerberosAuthenticationPassword: "",
          kerberosImpersonationUsername: "",
          kerberosImpersonationPassword: ""
        }
      };
    }

    function loadShimDropDowns() {
      vm.shimVendor = vm.data.model.shimVendor;
      vm.shimVersions = getShimVersions(vm.shimVendor);
      vm.shimVersion = vm.data.model.shimVersion;
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
            //if name is returned it already exists
            if (name === res.data.name) {
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

    function next() {
      if (vm.data.model.oldName === vm.data.model.name) {
        create();
      } else {
        var promise = checkDuplicateName(vm.data.model.name);
        promise.then(
          create,
          function () {
            displayOverwriteDialog(true);
          }
        );
      }
    }

    function create() {
      dataService.getSecure().then(function (res) {
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
            return (!vm.data.model || !vm.data.model.name) ||
              !((vm.data.model.hdfsHost && vm.data.model.hdfsPort) ||
                (vm.data.model.jobTrackerHost && vm.data.model.jobTrackerPort) ||
                (vm.data.model.zooKeeperHost && vm.data.model.zooKeeperPort) ||
                vm.data.model.oozieUrl ||
                vm.data.model.kafkaBootstrapServers);
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
    name: "newEdit",
    options: options
  };

});
