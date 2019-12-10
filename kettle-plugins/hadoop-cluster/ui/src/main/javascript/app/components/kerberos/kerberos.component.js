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

  kerberosController.$inject = ["$location", "$state", "$q", "$stateParams", "fileService"];

  function kerberosController($location, $state, $q, $stateParams, fileService) {
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

      vm.keytabAuthPathLabel = i18n.get('kerberos.keytab.auth.path.label');
      vm.keytabImpersonatePathLabel = i18n.get('kerberos.keytab.impersonate.path.label');

      if (!vm.data.model.kerberosSubType) {
        vm.data.model.kerberosSubType = vm.kerberosSubTypes.PASSWORD;
      }

      if (vm.data.model.keytabAuthFile) {
        vm.keytabAuthenticationFile = [{name: vm.data.model.keytabAuthFile}];
      }

      if (vm.data.model.keytabImpFile) {
        vm.keytabImpersonationFile = [{name: vm.data.model.keytabImpFile}];
      }

      var keytabAuthFile = fileService.getKeytabAuthFile();
      if (keytabAuthFile) {
        vm.keytabAuthenticationFile = [keytabAuthFile];
      }

      var keytabImpFile = fileService.getKeytabImpFile();
      if (keytabImpFile) {
        vm.keytabImpersonationFile = [keytabImpFile];
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
              return !vm.keytabAuthenticationFile;
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
              setFileServiceKeytabFiles();
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
            setFileServiceKeytabFiles();
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

    function setFileServiceKeytabFiles() {
      //UI-router doesn't work to pass files between states, use fileservice to store the file(s), they are later
      //retrieved by the helperService before passing the request to the server.
      if(vm.keytabAuthenticationFile && vm.keytabAuthenticationFile[0] instanceof File) {
        fileService.setKeytabAuthFile(vm.keytabAuthenticationFile[0]);
      }
      if(vm.keytabImpersonationFile && vm.keytabImpersonationFile[0] instanceof File) {
        fileService.setKeytabImpFile(vm.keytabImpersonationFile[0]);
      }
    }

    function clearKerberosPasswordValues() {
      vm.data.model.kerberosAuthenticationUsername = "";
      vm.data.model.kerberosAuthenticationPassword = "";
      vm.data.model.kerberosImpersonationUsername = "";
      vm.data.model.kerberosImpersonationPassword = "";
    }

    function clearKerberosKeytabValues() {
      fileService.setKeytabAuthFile(null);
      fileService.setKeytabImpFile(null);
    }

  }

  return {
    name: "kerberos",
    options: options
  };

});
