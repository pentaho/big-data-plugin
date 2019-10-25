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
  'text!./status.html',
  'pentaho/i18n-osgi!hadoopCluster.messages',
  'css!./status.css'
], function (template, i18n) {

  'use strict';

  var options = {
    bindings: {},
    controllerAs: "vm",
    template: template,
    controller: statusController
  };

  statusController.$inject = ["$state", "$stateParams"];

  function statusController($state, $stateParams) {
    var vm = this;
    vm.$onInit = onInit;
    vm.onCreateNew = onCreateNew;
    vm.onEditConnection = onEditConnection;
    vm.onTestCluster = onTestCluster;
    vm.getStatusImage = getStatusImage;
    vm.getOverallStatus = getOverallStatus;

    function onInit() {
      vm.data = $stateParams.data;

      vm.question = i18n.get('status.question');
      vm.createNewCluster = i18n.get('status.new');
      vm.editCluster = i18n.get('status.edit');
      vm.testCluster = i18n.get('status.test');
      vm.closeLabel = i18n.get('controls.close.label');

      vm.overallStatus = getOverallStatus();
      vm.overallStatusImage = vm.getStatusImage(vm.overallStatus);
      vm.overallStatusHeader = i18n.get('status.header.' + vm.overallStatus);
      vm.overallStatusDescription = i18n.get('status.description.' + vm.overallStatus);

      vm.buttons = getButtons();
    }

    function onCreateNew() {
      $state.go("import");
    }

    function onEditConnection() {
      vm.data.state = "edit";
      $state.go("new-edit", {data: vm.data, transition: "slideRight"});
    }

    function onTestCluster() {
      $state.go("testing", {data: vm.data, transition: "slideLeft"});
    }

    function getOverallStatus() {
      if (vm.data.created === false) {
        return "import.fail";
      }

      var lowestCategory = "Pass";
      for (var i = 0; i < vm.data.model.testCategories.length; i++) {
        var testCategoryStatus = vm.data.model.testCategories[i].categoryStatus;
        if (testCategoryStatus === "Warning") {
          lowestCategory = "Warning";
        } else if (testCategoryStatus === "Fail") {
          lowestCategory = testCategoryStatus;
          break;
        }
      }
      return lowestCategory.toLowerCase();
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
        case "import.fail":
          return "img/fail.svg";
        case "pass":
          return "img/success.svg";
        case "fail":
          return "img/fail.svg";
        default:
          return "img/warning.svg";
      }
    }
  }

  return {
    name: "status",
    options: options
  };

});
