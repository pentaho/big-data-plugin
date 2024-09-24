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
  'text!./controls.html',
  'css!./controls.css'
], function (template) {

  'use strict';

  var options = {
    bindings: {
      buttons: "<",
      helpLink: "<"
    },
    controllerAs: "vm",
    template: template,
    controller: controlsController
  };

  function controlsController() {
    var vm = this;
    vm.getRightButtons = getRightButtons;

    function getButtonsByPosition(position) {
      var buttons = [];
      if (vm.buttons) {
        for (var i = 0; i < vm.buttons.length; i++) {
          if (vm.buttons[i].position === position) {
            buttons.push(vm.buttons[i]);
          }
        }
      }
      return buttons;
    }

    function getRightButtons() {
      return getButtonsByPosition("right");
    }

  }


  return {
    name: "controls",
    options: options
  };

});
