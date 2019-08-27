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
    vm.checkConnectionName = checkConnectionName;
    vm.openFileBrowser = openFileBrowser;
    vm.checkConfigurationPath = checkConfigurationPath;
    vm.configurationType = null;
    vm.name = "";
    vm.configurationPath = "";
    var loaded = false;
    vm.selectConfigPathButtonLabel = "";
    vm.selectConfigPathHref = "location.href='http://localhost:9051/@pentaho/di-plugin-file-open-save-new@9.0.0.0-SNAPSHOT/index.html#/open?provider=vfs'";

    function onInit() {
      vm.clusterNameLabel = i18n.get('cluster.intro.clusterName.label');
      vm.specifyConfigurationLabel = i18n.get('cluster.intro.specify.configuration.label');
      vm.title = i18n.get('cluster.intro.new.header');
      vm.configurationTypes = [i18n.get('cluster.intro.import.ccfg'), i18n.get('cluster.intro.provide.site.xml')];
      //vm.specifyConfiguration = vm.configurationTypes[0];
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



      //TODO: first needs to be replaced to show the ccfg file selection path and button
      //TODO: second if using hadoopConfigFolderPath show the folder selection path and button

      // if (!vm.data.model || vm.data.model.type !== option.value) {
      //   dataService.getFields(option.value).then(function (res) {
      //     var name = vm.data.model.name;
      //     var description = vm.data.model.description;
      //     vm.data.model = res.data;
      //     vm.data.model.name = name;
      //     vm.data.model.description = description;
      //     vm.next = vm.data.model.type + "step1";
      //     vm.data.state = "new";
      //     vm.data.isSaved = false;
      //   });
      // }


    }

    function openFileBrowser() {
      window.open("http://localhost:9051/@pentaho/di-plugin-file-open-save-new@9.0.0.0-SNAPSHOT/index.html#/open?provider=vfs");
    }

    function selectFile() {

    }

    function checkConnectionName() {
      vm.resetErrorMsg();
      vm.name = vm.name.replace(/[^\w\s]/g, '');
    }

    function checkConfigurationPath() {
      //TODO: implement
    }

    function validateName() {
      return $q(function (resolve, reject) {
        if (vm.data.state === "edit" || vm.data.isSaved) {
          if (vm.name !== vm.data.model.name) {
            vm.data.name = vm.data.model.name;
            vm.data.model.name = vm.name;
          }
          resolve(true);
        } else {
          dataService.exists(vm.name).then(function (res) {
            var isValid = !res.data;
            if (!isValid) {
              vm.errorMessage = {
                type: "error",
                text: i18n.get('connections.intro.name.error', {
                  name: vm.name
                })
              }
            } else {
              vm.data.model.name = vm.name;
            }
            resolve(isValid);
          });
        }
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
        label: vm.data.state === "modify" ? i18n.get('connections.controls.applyLabel') : i18n.get('connections.controls.nextLabel'),
        class: "primary",
        isDisabled: function () {
          return !vm.data.model || !vm.data.model.type || !vm.name;
        },
        position: "right",
        onClick: function () {
          validateName().then(function (isValid) {
            if (isValid) {
              $state.go(vm.data.state === "modify" ? 'summary' : vm.next, {data: vm.data, transition: "slideLeft"});
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
