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
  'text!./credentials.html',
  'pentaho/i18n-osgi!hadoopCluster.messages',
  'css!./credentials.css'
], function (template, i18n) {

  'use strict';

  var options = {
    bindings: {
      usernameLabel: "<?", //optional - has default
      passwordLabel: "<?", // optional - has default
      username: "=",
      password: "="
    },
    controllerAs: "vm",
    template: template,
    controller: credentialsController
  };

  credentialsController.$inject = ["$document", "$scope"];

  function credentialsController($document, $scope) {
    var vm = this;
    vm.$onInit = onInit;

    vm.variableImage = "img/variable.svg";

    function onInit() {
      if (!vm.usernameLabel) {
        vm.usernameLabel = i18n.get('credentials.username.label');
      }
      if (!vm.passwordLabel) {
        vm.passwordLabel = i18n.get('credentials.password.label');
      }
    }
  }

  return {
    name: "credentials",
    options: options
  };

});
