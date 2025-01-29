/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


define([
  'text!./testing.html',
  'pentaho/i18n-osgi!hadoopCluster.messages'
], function (template, i18n) {

  'use strict';

  var options = {
    bindings: {},
    controllerAs: "vm",
    template: template,
    controller: testingController
  };

  testingController.$inject = ["$state", "$timeout", "$stateParams", "dataService", "$location"];

  function testingController($state, $timeout, $stateParams, dataService, $location) {
    var vm = this;
    vm.$onInit = onInit;

    function onInit() {
      vm.data = $stateParams.data;

      vm.almostDone = i18n.get('progress.almostdone');
      vm.message = i18n.get('testing.message');
      $timeout(function () {
        if(vm.data) {
          if (vm.data.created === true) {
            dataService.runTests(vm.data.model.name).then(function (res) {
              vm.data.model.testCategories = res.data;
              $state.go("test-results", {data: vm.data});
            });
          }
        } else {
          vm.data = {};
          vm.data.model = {};
          dataService.runTests($location.search().name).then(function (res) {
            vm.data.model.testCategories = res.data;
            vm.data.hideBack = true;
            $state.go("test-results", {data: vm.data, transition: "slideRight"});
          });
        }
      }, 500);
    }
  }

  return {
    name: "testing",
    options: options
  };

});
