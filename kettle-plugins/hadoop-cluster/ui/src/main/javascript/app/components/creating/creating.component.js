/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

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

        var process;

        switch (vm.data.type) {
          case "new":
            process = dataService.createNamedCluster;
            break;
          case "edit":
            process = dataService.editNamedCluster;
            break;
          case "duplicate":
            process = dataService.duplicateNamedCluster;
            break;
          case "import":
            process = dataService.importNamedCluster;
            break;
          default:
            vm.data.created = false;
            $state.go("status", {data: vm.data});
        }

        process(vm.data).then(
          function (res) {
            return processResultAndTest(res);
          },
          function (error) {
            vm.data.created = false;
            $state.go("status", {data: vm.data});
          });

      }, 500);
    }

    function processResultAndTest(res) {
      //namedCluster is returned on success, otherwise there was an error
      if (res && res.data && res.data.namedCluster && 0 !== res.data.namedCluster.length) {
        vm.data.created = true;
        dataService.runTests(vm.data.model.name)
        .then(function (res) {
          vm.data.model.testCategories = res.data;
          $state.go("status", {data: vm.data});
        });
      } else {
        vm.data.created = false;
        $state.go("status", {data: vm.data});
      }
    }

  }

  return {
    name: "creating",
    options: options
  };

});
