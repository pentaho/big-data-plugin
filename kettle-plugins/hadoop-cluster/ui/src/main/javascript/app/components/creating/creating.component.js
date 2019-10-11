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

        switch(vm.data.model.type) {
          case "new":
            //create a new new json to pass only the data required by the POST call
            var thinNameClusterModel = {
                name: vm.data.model.clusterName,
                shimVendor: vm.data.model.shimName,
                shimVersion: vm.data.model.shimVersion,
                importPath: encodeURIComponent(vm.data.model.importPath),
                hdfsHost: vm.data.model.hdfsHostname,
                hdfsPort: vm.data.model.hdfsPort,
                hdfsUsername: vm.data.model.hdfsUsername,
                hdfsPassword: vm.data.model.hdfsPassword,
                jobTrackerHost: vm.data.model.jobTrackerHostname,
                jobTrackerPort: vm.data.model.jobTrackerPort,
                zooKeeperHost: vm.data.model.zooKeeperHostname,
                zooKeeperPort: vm.data.model.zooKeeperPort,
                oozieUrl: vm.data.model.oozieHostname,
                kafkaBootstrapServers: vm.data.model.kafkaBootstrapServers
            };
            dataService.createNamedCluster(thinNameClusterModel)
            .then(
              function (res) {
                return processResultAndTest(res);
              },
              function (error) {
                vm.data.model.created = false;
                $state.go("status", {data: vm.data});
              });
            break;
          case "import":
            dataService.importNamedCluster(vm.data.model.clusterName, encodeURIComponent(vm.data.model.importPath), vm.data.model.shimName,
              vm.data.model.shimVersion)
            .then(function (res) {
              return processResultAndTest(res);
            });
            break;
          default:
            vm.data.model.created = false;
            $state.go("status", {data: vm.data});
        }
      }, 500);
    }

    function processResultAndTest(res) {
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
    }

  }

  return {
    name: "creating",
    options: options
  };

});
