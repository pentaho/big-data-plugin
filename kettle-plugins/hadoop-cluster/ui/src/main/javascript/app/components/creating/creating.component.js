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
  'text!./creating.html',
  'pentaho/i18n-osgi!hadoopCluster.messages'
], function (template, i18n) {

  'use strict';

  var options = {
    bindings: {},
    controllerAs: "vm",
    template: template,
    controller: creatingController
  };

  creatingController.$inject = ["$state", "$timeout", "$stateParams", "dataService"];

  function creatingController($state, $timeout, $stateParams, dataService) {
    var vm = this;
    vm.$onInit = onInit;

    function onInit() {
      vm.data = $stateParams.data;

      vm.almostDone = i18n.get('progress.almostdone');
      vm.message = i18n.get('creating.message');
      $timeout(function () {
        var type = "ccfg";
        if (i18n.get('hadoop.cluster.site.xml.type') === vm.data.model.configurationType) {
          type = "site";
        }
        dataService.newNamedCluster(vm.data.model.clusterName, type, vm.data.model.configPath, vm.data.model.shimName,
          vm.data.model.shimVersion)
          .then(function (res) {
            //namedCluster is returned on success, otherwise there was an error
            if (res && res.data && res.data.namedCluster && 0 !== res.data.namedCluster.length) {
              vm.data.model.created = true;
              dataService.runTests(vm.data.model.clusterName)
              .then(function (res) {
                vm.data.model.testCategories = res.data;
                $state.go("status", {data: vm.data});
              });
            } else {
              vm.data.model.created = false;
              $state.go("status", {data: vm.data});
            }
          });
      }, 500);
    }
  }

  return {
    name: "creating",
    options: options
  };

});
