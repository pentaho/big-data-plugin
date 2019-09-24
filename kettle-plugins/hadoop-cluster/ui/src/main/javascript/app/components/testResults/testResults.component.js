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
  'text!./testResults.html',
  'pentaho/i18n-osgi!hadoopCluster.messages',
  'css!./testResults.css'
], function (template, i18n) {

  'use strict';

  var options = {
    bindings: {},
    controllerAs: "vm",
    template: template,
    controller: testResultsController
  };

  testResultsController.$inject = ["$state", "$stateParams", "dataService"];

  function testResultsController($state, $stateParams, dataService) {
    var vm = this;
    vm.$onInit = onInit;
    vm.title = i18n.get('cluster.hadoop.testResults.label');

    function onInit() {
      vm.data = $stateParams.data ? $stateParams.data : {};
      vm.clusterNameLabel = i18n.get('cluster.hadoop.clusterName.label');
      vm.buttons = getButtons();
    }

    function getButtons() {
      return [
        {
          label: i18n.get('cluster.controls.closeLabel'),
          class: "primary",
          position: "right",
          onClick: function () {
            close();
          }
        },
        {
          label: i18n.get('cluster.controls.backLabel'),
          class: "primary",
          position: "right",
          onClick: function () {
            $state.go('status', {data: vm.data, transition: "slideRight"});
          }
        }];
    }
  }

  return {
    name: "testResults",
    options: options
  };
});
