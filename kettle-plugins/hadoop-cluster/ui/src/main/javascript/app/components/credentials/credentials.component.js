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
