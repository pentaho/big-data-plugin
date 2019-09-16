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
  'pentaho/i18n-osgi!hadoop-cluster.messages',
  'css!./creating.css'
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

      vm.almostDone = i18n.get('cluster.creating.almostdone.label');
      vm.message = i18n.get('cluster.creating.message');
      $timeout(function () {
        var type = "ccfg";
        if (i18n.get('cluster.hadoop.provide.site.xml') === vm.data.model.configurationType) {
          type = "site";
        }
        dataService.newNamedCluster(vm.data.model.clusterName, type,
            vm.data.model.currentPath, vm.data.model.shimName, vm.data.model.shimVersion)
            .then(function (response) {
              $state.go("success", {data: vm.data});
            });
        //TODO: handle cluster creation failure
      }, 1000);
    }
  }

  return {
    name: "creating",
    options: options
  };

});
