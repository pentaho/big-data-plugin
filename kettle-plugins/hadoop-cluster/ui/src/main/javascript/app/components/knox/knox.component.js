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
  'text!./knox.html',
  'pentaho/i18n-osgi!hadoopCluster.messages'
], function (template, i18n) {

  'use strict';

  var options = {
    bindings: {},
    controllerAs: "vm",
    template: template,
    controller: knoxController
  };

  knoxController.$inject = ["$location", "$state", "$q", "$stateParams"];

  function knoxController($location, $state, $q, $stateParams) {
    var vm = this;
    vm.$onInit = onInit;

    function onInit() {
      vm.data = $stateParams.data ? $stateParams.data : {};

      vm.header = i18n.get(vm.data.type + ".header");
      vm.gatewayUrlLabel = i18n.get('knox.gateway.url.label');
      vm.gatewayUsernameLabel = i18n.get('knox.gateway.username.label');
      vm.gatewayPasswordLabel = i18n.get('knox.gateway.password.label');
      vm.helpLink = i18n.get('knox.help');

      vm.buttons = getButtons();
    }

    function getButtons() {
      return [
        {
          label: i18n.get('controls.next.label'),
          class: "primary",
          isDisabled: function () {
              return !(vm.data.model.gatewayUrl && vm.data.model.gatewayUsername && vm.data.model.gatewayPassword);
          },
          position: "right",
          onClick: function () {
            $state.go('creating', {data: vm.data, transition: "slideLeft"});
          }
        },
        {
          label: i18n.get('controls.back.label'),
          class: "primary",
          position: "right",
          onClick: function () {
            $state.go('security', {data: vm.data, transition: "slideRight"});
          }
        },
        {
          label: i18n.get('controls.cancel.label'),
          class: "primary",
          position: "right",
          onClick: close
        }];
    }
  }

  return {
    name: "knox",
    options: options
  };

});
