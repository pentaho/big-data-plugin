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
