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

  testResultsController.$inject = ["$state", "$stateParams"];

  function testResultsController($state, $stateParams) {
    var vm = this;
    vm.$onInit = onInit;
    vm.title = i18n.get('test.results.title');
    vm.helpLink = i18n.get('test.results.help');

    function onInit() {
      vm.data = $stateParams.data ? $stateParams.data : {};
      vm.buttons = getButtons();
    }

    function getButtons() {
      var buttons = [
        {
          label: i18n.get('controls.close.label'),
          class: "primary",
          position: "right",
          onClick: function () {
            close();
          }
        }
      ];
      if(!vm.data.hideBack) {
        buttons[1] =
          {
            label: i18n.get('controls.back.label'),
            class: "primary",
            position: "right",
            onClick: function () {
              $state.go('status', {data: vm.data, transition: "slideRight"});
            }
          }
      }
      return buttons;
    }
  }

  return {
    name: "testResults",
    options: options
  };
});
