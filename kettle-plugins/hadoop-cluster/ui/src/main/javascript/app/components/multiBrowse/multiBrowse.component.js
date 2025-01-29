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
  'text!./multiBrowse.html',
  'pentaho/i18n-osgi!hadoopCluster.messages',
  'css!./multiBrowse.css'
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
    controller: multiBrowseController
  };

  multiBrowseController.$inject = [];

  function multiBrowseController() {
    var vm = this;
    vm.$onInit = onInit;
    vm.removeFile = removeFile;
    vm.keydownBrowseButton = keydownBrowseButton;
    vm.keyDownOnRemove = keyDownOnRemove;

    function onInit() {
      if (!vm.placeholder) {
        vm.placeholder = i18n.get('multi.browse.placeholder.default');
      }
      if (!vm.buttonLabel) {
        vm.buttonLabel = i18n.get('multi.browse.button.default');
      }
      vm.removeLabel = i18n.get('multi.browse.remove');
    }

    function removeFile(file) {
      vm.model = vm.model.filter(
        function (e) {
          return e !== file;
        });
    }

    function keydownBrowseButton(e) { //Hitting enter when browse button has focus works like clicking the button
      if (e.which == 13 || e.keyCode == 13 ) {
        var button = document.getElementById("browseButton");
        button.click();
        return false;
      }
      return true;
    }

    function keyDownOnRemove(e,file) {
      if (e.which == 13 || e.keyCode == 13 ) {
        removeFile(file);
        return false;
      }
      return true;
    }

  }

  return {
    name: "multiBrowse",
    options: options
  };

});
