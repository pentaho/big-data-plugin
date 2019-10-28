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
    'text!./addDriver.html',
    'pentaho/i18n-osgi!hadoopCluster.messages'
  ], function (template, i18n) {

    'use strict';

    var options = {
      bindings: {},
      controllerAs: "vm",
      template: template,
      controller: addDriverController
    };

    addDriverController.$inject = ["$state"];

    function addDriverController($state) {
      var vm = this;
      vm.$onInit = onInit;

      function onInit() {

        setDialogTitle(i18n.get('hadoop.cluster.title'));

        vm.browseType = "file";
        vm.header = i18n.get('add.driver.header');
        vm.driverInstructionsLabel = i18n.get('add.driver.instructions.label');
        vm.driverSupportMatrixLinkText = i18n.get('add.driver.support.matrix.line.text');
        vm.fileLabel = i18n.get('add.driver.file.label');

        vm.data = {
          type: "driver",
          driverPath: ""
        };

        vm.buttons = getButtons();
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
              return !vm.data.driverPath;
            },
            position: "right",
            onClick: function () {
              $state.go('installing-driver', {data: vm.data, transition: "slideLeft"});
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
      name: "addDriver",
      options: options
    };

  }
);
