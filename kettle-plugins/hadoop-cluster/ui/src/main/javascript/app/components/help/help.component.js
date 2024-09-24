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
  'text!./help.html',
  'pentaho/i18n-osgi!hadoopCluster.messages'
], function (template, i18n) {

  'use strict';

  var options = {
    bindings: {
      link: "<"
    },
    controllerAs: "vm",
    template: template,
    controller: helpController
  };

  helpController.$inject = ["dataService"];

  function helpController(dataService) {
    var vm = this;
    vm.$onInit = onInit;
    vm.openLink = openLink;
    vm.helpKeyDown = helpKeyDown;

    /**
     * The $onInit hook of components lifecycle which is called on each controller
     * after all the controllers on an element have been constructed and had their
     * bindings initialized. We use this hook to put initialization code for our controller.
     */
    function onInit() {
      vm.helpLabel = i18n.get('help.helpLabel');
    }

    function openLink() {
      open(vm.link ,'','height=600,width=800,scrollbars=yes,toolbar=no,status=no,menubar=no,location=no,resizable=yes');
    }

    function helpKeyDown( e ) {
      if (e.which == 13 || e.keyCode == 13 ) {
        openLink();
        return false;
      }
      return true;
    }
  }

  return {
    name: "help",
    options: options
  };

});
