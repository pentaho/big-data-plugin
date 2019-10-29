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
  'text!./driverStatus.html',
  'pentaho/i18n-osgi!hadoopCluster.messages'
], function (template, i18n) {

  'use strict';

  var options = {
    bindings: {},
    controllerAs: "vm",
    template: template,
    controller: driverStatusController
  };

  driverStatusController.$inject = ["$state", "$stateParams"];

  function driverStatusController($state, $stateParams) {
    var vm = this;
    vm.$onInit = onInit;
    vm.getStatusImage = getStatusImage;
    vm.getOverallStatus = getOverallStatus;

    function onInit() {
      vm.data = $stateParams.data;

      vm.closeLabel = i18n.get('controls.close.label');

      vm.overallStatus = getOverallStatus();
      vm.overallStatusImage = vm.getStatusImage(vm.overallStatus);
      vm.overallStatusHeader = i18n.get('driver.status.header.' + vm.overallStatus);
      vm.overallStatusDescription = i18n.get('driver.status.description.' + vm.overallStatus);

      vm.buttons = getButtons();
    }

    function getOverallStatus() {
      if (vm.data.installed === true) {
        return "success";
      } else {
        return "fail";
      }
    }

    function getButtons() {
      return [{
        label: i18n.get('controls.close.label'),
        class: "primary",
        position: "right",
        onClick: function () {
          close();
        }
      }];
    }

    function getStatusImage(status) {
      switch (status) {
        case "success":
          return "img/success.svg";
        default:
          return "img/fail.svg";
      }
    }
  }

  return {
    name: "driverStatus",
    options: options
  };

});
