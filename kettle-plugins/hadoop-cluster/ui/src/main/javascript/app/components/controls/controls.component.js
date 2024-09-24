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
