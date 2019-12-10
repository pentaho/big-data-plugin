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
  'text!./security.html',
  'pentaho/i18n-osgi!hadoopCluster.messages',
  'css!./security.css'
], function (template, i18n) {

  'use strict';

  var options = {
    bindings: {},
    controllerAs: "vm",
    template: template,
    controller: securityController
  };

  securityController.$inject = ["$location", "$state", "$q", "$stateParams", "fileService"];

  function securityController($location, $state, $q, $stateParams, fileService) {
    var vm = this;
    vm.$onInit = onInit;

    function onInit() {
      vm.securityType = {
        NONE: "None",
        KERBEROS: "Kerberos",
        KNOX: "Knox"
      };

      vm.data = $stateParams.data ? $stateParams.data : {};

      vm.header = i18n.get(vm.data.type + ".header");
      vm.securityTypeLabel = i18n.get('security.type.label');

      if (!vm.data.model.securityType ||
        (vm.data.model.securityType === vm.securityType.KNOX && vm.data.model.shimVendor !== "Hortonworks")) {
        vm.data.model.securityType = vm.securityType.NONE;
      }
      vm.buttons = getButtons();
    }

    function getButtons() {
      return [
        {
          label: i18n.get('controls.next.label'),
          class: "primary",
          position: "right",
          onClick: function () {
            switch (vm.data.model.securityType) {
              case vm.securityType.KERBEROS:
                clearKnoxValues();
                $state.go('kerberos', {data: vm.data, transition: "slideLeft"});
                break;
              case vm.securityType.KNOX:
                clearKerberosValues();
                $state.go('knox', {data: vm.data, transition: "slideLeft"});
                break;
              default:
                clearKnoxValues();
                clearKerberosValues();
                $state.go('creating', {data: vm.data, transition: "slideLeft"});
            }
          }
        },
        {
          label: i18n.get('controls.back.label'),
          class: "primary",
          position: "right",
          onClick: function () {
            switch (vm.data.type) {
              case "new":
                $state.go('new-edit', {data: vm.data, transition: "slideRight"});
                break;
              case "edit":
                $state.go('new-edit', {data: vm.data, transition: "slideRight"});
                break;
              case "duplicate":
                $state.go('new-edit', {data: vm.data, transition: "slideRight"});
                break;
              case "import":
                $state.go('import', {data: vm.data, transition: "slideRight"});
                break;
            }
          }
        },
        {
          label: i18n.get('controls.cancel.label'),
          class: "primary",
          position: "right",
          onClick: close
        }];
    }

    function clearKerberosValues() {
      vm.data.model.kerberosSubType = "";
      vm.data.model.kerberosAuthenticationUsername = "";
      vm.data.model.kerberosAuthenticationPassword = "";
      vm.data.model.kerberosImpersonationUsername = "";
      vm.data.model.kerberosImpersonationPassword = "";
      fileService.setKeytabAuthFile(null);
      fileService.setKeytabImpFile(null);
    }

    function clearKnoxValues() {
      vm.data.model.gatewayUrl = "";
      vm.data.model.gatewayUsername = "";
      vm.data.model.gatewayPassword = "";
    }
  }

  return {
    name: "security",
    options: options
  };

});
