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
  'text!./browse.html',
  'pentaho/i18n-osgi!hadoopCluster.messages',
  'css!./browse.css'
], function (template, i18n) {

  'use strict';

  var options = {
    bindings: {
      label: "<",
      model: "=",
      buttonLabel: "<?", //optional - has defaults
      placeholder: "<?" // optional - has defaults
    },
    controllerAs: "vm",
    template: template,
    controller: browseController
  };

  browseController.$inject = ["$timeout"];

  function browseController($timeout) {
    var vm = this;
    vm.$onInit = onInit;
    vm.keyDown = keyDown;

    function onInit() {
      if (!vm.placeholder) {
        vm.placeholder = i18n.get('browse.placeholder.default');
      }
      if (!vm.buttonLabel) {
        vm.buttonLabel = i18n.get('browse.button.default');
      }
    }

    function keyDown(e,id) {
      if (e.which == 13 || e.keyCode == 13 ) {
        $timeout(function() {
          angular.element('#browse_'+id).trigger('click');
        },0);
        return false;
      }
      return true;
    }

  }

  return {
    name: "browse",
    options: options
  };

});
