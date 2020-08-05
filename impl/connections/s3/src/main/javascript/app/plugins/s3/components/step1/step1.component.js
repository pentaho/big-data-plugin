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
  'text!./step1.html',
  'pentaho/i18n-osgi!pentaho-s3.messages',
  'css!./step1.css'
], function (template, i18n) {

  'use strict';

  var options = {
    bindings: {},
    controllerAs: "vm",
    template: template,
    controller: step1Controller
  };

  step1Controller.$inject = ["$state", "$stateParams", "dataService", "$q"];

  function step1Controller($state, $stateParams, dataService, $q) {
    var vm = this;
    vm.$onInit = onInit;
    vm.canNext = canNext;
    vm.doTest = doTest;
    vm.onSelectOfAuthType = onSelectOfAuthType;
    vm.onSelectOfConnectionType = onSelectOfConnectionType;
    vm.onSelectRegion = onSelectRegion;
    vm.onBrowse = onBrowse;
    vm.type = 0;
    vm.connectionType = 0;
    vm.connectionTypes = [{
      value: 0,
      label: i18n.get('S3.Label.ConnectionType.AWS')
    }, {
      value: 1,
      label: i18n.get('S3.Label.ConnectionType.Minio')
    }];

    vm.authTypes = [{
      value: 0,
      label: i18n.get('S3.Label.Type.AccessKeySecretKey')
    }, {
      value: 1,
      label: i18n.get('S3.Label.Type.CredentialsFile')
    }];

    function onInit() {
      vm.data = $stateParams.data ? $stateParams.data : {};
      vm.connectionDetails = i18n.get('Connection.Label.ConnectionDetails');
      vm.authenticationType = i18n.get('S3.Label.AuthenticationType');
      vm.profileName = i18n.get('S3.Label.ProfileName');
      vm.regionLabel = i18n.get('S3.Label.Region');
      vm.accessKey = i18n.get('S3.Label.AccessKey');
      vm.secretKey = i18n.get('S3.Label.SecretKey');
      vm.sessionToken = i18n.get('S3.Label.SessionToken.Label');
      vm.fileLocation = i18n.get('S3.Label.FileLocation');
      vm.endpoint = i18n.get('S3.Label.Endpoint');
      vm.pathStyleAccess = i18n.get('S3.Label.PathStyleAccess');
      vm.signatureVersion = i18n.get('S3.Label.SignatureVersion');
      vm.defaultS3Config = i18n.get('S3.Label.DefaultS3Config');
      vm.yes = i18n.get('S3.Label.Yes');
      vm.no = i18n.get('S3.Label.No');
      vm.browse = i18n.get('S3.Label.Browse');
      vm.otherLabel = i18n.get('S3.Label.Other');
      vm.connectionTypeLabel = i18n.get('S3.Label.ConnectionType');
      vm.buttons = getButtons();

      if (!vm.data.model.connectionType) {
        vm.connectionType = 0;
      } else {
        vm.connectionType = parseInt(vm.data.model.connectionType);
      }

      if (!vm.data.model.authType) {
        vm.type = 0;
      } else {
        vm.type = parseInt(vm.data.model.authType);
      }

      if (vm.data.model.region) {
        vm.region = vm.data.model.region;
      }

      vm.regions = [];
      for (var i = 0; i < vm.data.model.regions.length; i++) {
        vm.regions.push({
          label: vm.data.model.regions[i],
          value: vm.data.model.regions[i],
        })
      }
    }

    function onSelectOfConnectionType(connectionType) {
      if (vm.connectionType !== parseInt(vm.data.model.connectionType)) {
        vm.data.model.endpoint = null;
        vm.data.model.pathStyleAccess = null;
        vm.data.model.signatureVersion = null;
        vm.data.model.defaultS3Config = null;
        vm.data.model.profileName = null;
        vm.data.model.credentialsFilePath = null;
        vm.region = null;
      }
      vm.connectionType = connectionType.value;      
    }

    function onSelectOfAuthType(type) {
      if (vm.type !== parseInt(vm.data.model.authType)) {
        vm.data.model.accessKey = null;
        vm.data.model.secretKey = null;
        vm.data.model.sessionToken = null;
        vm.data.model.endpoint = null;
        vm.data.model.pathStyleAccess = null;
        vm.data.model.signatureVersion = null;
        vm.data.model.defaultS3Config = null;
        vm.data.model.profileName = null;
        vm.data.model.credentialsFilePath = null;
        vm.region = null;
      }
      vm.type = type.value;
    }

    function onSelectRegion(region) {
      vm.region = region.value;
    }

    function onBrowse() {
      try {
        var key = browse();
        if (key) {
          vm.data.model.credentialsFilePath = key;
        }
      } catch (e) {
        vm.data.model.credentialsFilePath = "/";
      }
    }

    function canNext() {
      if (vm.data && vm.data.model) {
        if (vm.connectionType === 0 && vm.type === 0) {
          return vm.data.model.accessKey && vm.data.model.secretKey;
        }
        if (vm.connectionType === 0 && vm.type === 1) {
          return vm.data.model.profileName && vm.data.model.credentialsFilePath;
        }
        if (vm.connectionType === 1) {
          return vm.data.model.accessKey && vm.data.model.secretKey && vm.data.model.endpoint && vm.data.model.pathStyleAccess && vm.data.model.signatureVersion;
        }
      }
      return false;
    }

    function doTest() {
      return $q(function(resolve, reject) {
        vm.data.model.authType = vm.type;
        vm.data.model.connectionType = vm.connectionType;
        vm.data.model.region = vm.region;

        if (!vm.data.model.pathStyleAccess) {
          vm.data.model.pathStyleAccess = "false";
        }

        if (!vm.data.model.defaultS3Config) {
          vm.data.model.defaultS3Config = "false";
        }

        dataService.testConnection(vm.data.model).then(function() {
          vm.message = {
            "type": "success",
            "text": vm.data.state === 'modify' ? i18n.get('Connection.Success.Label.Apply') : i18n.get('Connection.Success.Label.Next')
          };
          resolve();
        }, function() {
          vm.message = {
            "type": "error",
            "text": i18n.get('Connection.Error.Label')
          };
          reject();
        });
      });
    }

    function getButtons() {
      var buttons = [];
      buttons.push({
        label: vm.data.state === "modify" ? i18n.get('Connection.Button.Label.Apply') : i18n.get('Connection.Button.Label.Next'),
        class: "primary",
        isDisabled: function() {
          return !canNext();
        },
        position: "right",
        onClick: function() {
          vm.data.model.authType = vm.type;
          vm.data.model.connectionType = vm.connectionType;
          vm.data.model.region = vm.region;
          $state.go("summary", {data: vm.data, transition: "slideLeft"});
        }
      });
      if (vm.data.state !== "modify") {
        buttons.push({
          label: i18n.get('Connection.Button.Label.Back'),
          class: "secondary",
          position: "right",
          onClick: function() {
            $state.go("intro", {data: vm.data, transition: "slideRight"});
          }
        });
      }
      buttons.push({
        label: i18n.get('Connection.Button.Label.Test'),
        class: "secondary",
        position: "middle",
        isWaiting: false,
        onClick: function() {
          this.isWaiting = true;
          var self = this;
          doTest().then(function() {
            self.isWaiting = false;
          }, function() {
            self.isWaiting = false;
          });
        }
      });
      return buttons;
    }
  }

  return {
    name: "s3step1",
    options: options
  };

});
