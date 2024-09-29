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
  'text!./driverStatus.html',
  'pentaho/i18n-osgi!hadoopCluster.messages'
], function (template, i18n) {

  'use strict';

  var options = {
    bindings: {},
    controllerAs: "vm",
    template: template,
    controller: driverStatusController
  };

  driverStatusController.$inject = ["$state", "$stateParams"];

  function driverStatusController($state, $stateParams) {
    var vm = this;
    vm.$onInit = onInit;
    vm.getStatusImage = getStatusImage;
    vm.getOverallStatus = getOverallStatus;

    function onInit() {
      vm.data = $stateParams.data;

      vm.closeLabel = i18n.get('controls.close.label');

      vm.overallStatus = getOverallStatus();
      vm.overallStatusImage = vm.getStatusImage(vm.overallStatus);
      vm.overallStatusHeader = i18n.get('driver.status.header.' + vm.overallStatus);
      vm.overallStatusDescription = i18n.get('driver.status.description.' + vm.overallStatus);
      vm.helpLink = i18n.get('driver.status.help');

      vm.buttons = getButtons();
    }

    function getOverallStatus() {
      if (vm.data.installed === true) {
        return "success";
      } else {
        return "fail";
      }
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
        case "success":
          return "img/success.svg";
        default:
          return "img/fail.svg";
      }
    }
  }

  return {
    name: "driverStatus",
    options: options
  };

});
