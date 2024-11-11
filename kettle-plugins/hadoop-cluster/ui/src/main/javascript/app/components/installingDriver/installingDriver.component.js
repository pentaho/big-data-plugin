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
