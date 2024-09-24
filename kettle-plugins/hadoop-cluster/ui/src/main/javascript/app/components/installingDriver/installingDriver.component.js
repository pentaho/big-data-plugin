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
  'text!./installingDriver.html',
  'pentaho/i18n-osgi!hadoopCluster.messages'
], function (template, i18n) {

  'use strict';

  var options = {
    bindings: {},
    controllerAs: "vm",
    template: template,
    controller: installingDriverController
  };

  installingDriverController.$inject = ["$state", "$timeout", "$stateParams", "dataService"];

  function installingDriverController($state, $timeout, $stateParams, dataService) {
    var vm = this;
    vm.$onInit = onInit;

    function onInit() {
      vm.data = $stateParams.data;

      vm.almostDone = i18n.get('progress.almostdone');
      vm.message = i18n.get('installing.driver.message');
      $timeout(function () {
        dataService.installDriver().then(
          function (res) {
              vm.data = res.data;
              $state.go("driver-status", {data: vm.data});
          },
          function (error) {
            vm.data.installed = false;
            $state.go("driver-status", {data: vm.data});
          });
      }, 500);
    }
  }

  return {
    name: "installingDriver",
    options: options
  };

});
