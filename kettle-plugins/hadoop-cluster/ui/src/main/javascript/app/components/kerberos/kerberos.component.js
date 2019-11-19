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
  'text!./kerberos.html',
  'pentaho/i18n-osgi!hadoopCluster.messages'
], function (template, i18n) {

  'use strict';

  var options = {
    bindings: {},
    controllerAs: "vm",
    template: template,
    controller: kerberosController
  };

  kerberosController.$inject = ["$location", "$state", "$q", "$stateParams"];

  function kerberosController($location, $state, $q, $stateParams) {
    var vm = this;
    vm.$onInit = onInit;
    vm.onSelectSubType = onSelectSubType;

    function onInit() {

      vm.kerberosSubTypes = {
        PASSWORD: "Password",
        KEYTAB: "Keytab"
      };

      vm.subTypes = [vm.kerberosSubTypes.PASSWORD, vm.kerberosSubTypes.KEYTAB];

      vm.data = $stateParams.data ? $stateParams.data : {};

      vm.header = i18n.get(vm.data.type + ".header");
      vm.subTypeLabel = i18n.get('kerberos.subType.label');

      vm.authUserLabel = i18n.get('kerberos.auth.username.label');
      vm.authPasswordLabel = i18n.get('kerberos.auth.password.label');
      vm.impersonateUserLabel = i18n.get('kerberos.impersonate.username.label');
      vm.impersonatePasswordLabel = i18n.get('kerberos.impersonate.password.label');

      vm.keytabBrowseType = "file";
      vm.keytabAuthPathLabel = i18n.get('kerberos.keytab.auth.path.label');
      vm.keytabImpersonatePathLabel = i18n.get('kerberos.keytab.impersonate.path.label');

      if (!vm.data.model.kerberosSubType) {
        vm.data.model.kerberosSubType = vm.kerberosSubTypes.PASSWORD;
      }

      vm.buttons = getButtons();
    }

    function onSelectSubType(value) {
      vm.data.model.kerberosSubType = value;
    }

    function getButtons() {
      return [
        {
          label: i18n.get('controls.next.label'),
          class: "primary",
          isDisabled: function () {
            if (vm.data.model.kerberosSubType === vm.kerberosSubTypes.KEYTAB) {
              return !(vm.data.model.keytabAuthenticationLocation || vm.data.model.keytabImpersonationLocation);
            } else {
              return (vm.data.model.kerberosAuthenticationUsername && !vm.data.model.kerberosAuthenticationPassword) ||
                (!vm.data.model.kerberosAuthenticationUsername && vm.data.model.kerberosAuthenticationPassword) ||
                (vm.data.model.kerberosImpersonationUsername && !vm.data.model.kerberosImpersonationPassword) ||
                (!vm.data.model.kerberosImpersonationUsername && vm.data.model.kerberosImpersonationPassword) ||
                (!vm.data.model.kerberosAuthenticationUsername && !vm.data.model.kerberosImpersonationUsername);
            }
          },
          position: "right",
          onClick: function () {
            if (vm.data.model.kerberosSubType === vm.kerberosSubTypes.KEYTAB) {
              clearKerberosPasswordValues();
            } else {
              clearKerberosKeytabValues();
            }
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

    function clearKerberosPasswordValues() {
      vm.data.model.kerberosAuthenticationUsername = "";
      vm.data.model.kerberosAuthenticationPassword = "";
      vm.data.model.kerberosImpersonationUsername = "";
      vm.data.model.kerberosImpersonationPassword = "";
    }

    function clearKerberosKeytabValues() {
      vm.data.model.keytabAuthenticationLocation = "";
      vm.data.model.keytabImpersonationLocation = "";
    }

  }

  return {
    name: "kerberos",
    options: options
  };

});
