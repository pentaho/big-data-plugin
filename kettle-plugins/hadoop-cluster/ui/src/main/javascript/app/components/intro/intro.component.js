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
  'text!./intro.html',
  'pentaho/i18n-osgi!hadoop-cluster.messages',
  'css!./intro.css'
], function (template, i18n) {

  'use strict';

  var options = {
    bindings: {},
    controllerAs: "vm",
    template: template,
    controller: introController
  };

  introController.$inject = ["$location", "$state", "$q", "$stateParams", "dataService"];

  function introController($location, $state, $q, $stateParams, dataService) {
    var vm = this;
    vm.$onInit = onInit;
    vm.onSelect = onSelect;
    vm.validateName = validateName;
    vm.resetErrorMsg = resetErrorMsg;
    vm.checkClusterName = checkClusterName;
    vm.onBrowse = onBrowse;
    vm.checkConfigurationPath = checkConfigurationPath;
    vm.configurationType = null;
    vm.configurationPath = "";
    var loaded = false;
    vm.selectConfigPathButtonLabel = "";
    vm.selectConfigPathHref = "location.href='http://localhost:9051/@pentaho/di-plugin-file-open-save-new@9.0.0.0-SNAPSHOT/index.html#/open?provider=vfs'";

    function onInit() {
      vm.data = $stateParams.data ? $stateParams.data : {};
      vm.clusterNameLabel = i18n.get('cluster.intro.clusterName.label');
      vm.specifyConfigurationLabel = i18n.get('cluster.intro.specify.configuration.label');
      vm.title = i18n.get('cluster.intro.new.header');
      vm.configurationTypes = [i18n.get('cluster.intro.import.ccfg'), i18n.get('cluster.intro.provide.site.xml')];
      vm.configurationType = vm.configurationTypes[0];
      vm.configurationPath = "";
      vm.configurationPathPlaceholder = i18n.get('cluster.intro.no.ccfg.selected.placeholder');
      vm.selectConfigPathButtonLabel = i18n.get('cluster.intro.selectCcfgFileButtonLabel');

      vm.next = "/";

      if ($stateParams.data) {
        //TODO: future implementation use ui-router for saving data between screens
        //vm.data = $stateParams.data;
      } else {

        setDialogTitle(i18n.get('cluster.intro.title'));

        vm.data = {
          model: {
            clusterName: "",
            configurationType: "",
            ccfgFilePath: "",
            hadoopConfigFolderPath: ""
          }
        };
      }


      vm.buttons = getButtons();
    }

    function resetErrorMsg() {
      if (!vm.data.isSaved) {
        vm.data.state = "new";
        vm.title = i18n.get('cluster.intro.new.header');
        setDialogTitle(i18n.get('cluster.intro.title'));
      }
      vm.errorMessage = null;
    }

    function onSelect(option) {
      vm.data.model.configurationType = option;
    }

    function onBrowse() {
      try {
        var path = browse();
        if (path) {
          vm.data.model.ccfgFilePath = path;
        }
      } catch (e) {
        vm.data.model.ccfgFilePath = "/";
      }
    }

    function checkClusterName() {
      //TODO: implement
    }

    function checkConfigurationPath() {
      //TODO: implement
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
      return [{
        label: i18n.get('connections.controls.nextLabel'),
        class: "primary",
        isDisabled: function () {
          return !vm.data.model || !vm.data.model.clusterName || !vm.data.model.configurationType || !vm.data.model.ccfgFilePath ;
        },
        position: "right",
        onClick: function () {
          validateName().then(function (isValid) {
            if (isValid) {
              $state.go('summary', {data: vm.data, transition: "slideLeft"});
            }
          });
        }
      }];
    }
  }

  return {
    name: "intro",
    options: options
  };

});
