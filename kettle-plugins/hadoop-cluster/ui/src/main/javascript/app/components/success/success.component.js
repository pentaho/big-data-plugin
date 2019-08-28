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
  'text!./success.html',
  'pentaho/i18n-osgi!hadoop-cluster.messages',
  'css!./success.css'
], function (template, i18n) {

  'use strict';

  var options = {
    bindings: {},
    controllerAs: "vm",
    template: template,
    controller: successController
  };

  successController.$inject = ["$state", "$stateParams"];

  function successController($state, $stateParams) {
    var vm = this;
    vm.$onInit = onInit;
    vm.onCreateNew = onCreateNew;
    vm.onEditConnection = onEditConnection;
    vm.onTestCluster = onTestCluster;

    function onInit() {
      vm.data = $stateParams.data;
      vm.congratulationsLabel = i18n.get('cluster.final.congratulationsLabel');
      vm.ready = i18n.get('cluster.final.readyCreate');
      vm.question = i18n.get('cluster.final.question');
      vm.createNewCluster = i18n.get('cluster.final.createNewCluster');
      vm.editCluster = i18n.get('cluster.final.editCluster');
      vm.testCluster = i18n.get('cluster.final.testCluster');
      vm.closeLabel = i18n.get('cluster.controls.closeLabel');
      vm.data.isSaved = true;
      vm.buttons = getButtons();
    }

    function onCreateNew() {
      $state.go("hadoopcluster");
    }

    function onEditConnection() {
      vm.data.state = "edit";
      $state.go("hadoopcluster", {data: vm.data, transition: "slideRight"});
    }

    function onTestCluster() {
      //TODO: test cluster and display test results
    }

    function getButtons() {
      return [{
        label: i18n.get('cluster.controls.closeLabel'),
        class: "primary",
        position: "right",
        onClick: function() {
          close();
        }
      }];
    }

    function setDialogTitle(title) {
      try {
        setTitle(title);
      } catch (e) {
        console.log(title);
      }
    }
  }

  return {
    name: "success",
    options: options
  };

});
