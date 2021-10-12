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
  'text!./selectBox.html',
  'css!./selectBox.css'
], function (template) {

  'use strict';

  var options = {
    bindings: {
      options: "<",
      type: "<",
      onSelect: "&"
    },
    controllerAs: "vm",
    template: template,
    controller: selectBoxController
  };

  selectBoxController.$inject = ["$document", "$scope"];

  function selectBoxController($document, $scope) {
    var vm = this;
    vm.$onChanges = onChanges;
    vm.selectOption = selectOption;
    vm.toggleOptions = toggleOptions;
    vm.onBodyClick = onBodyClick;
    vm.isShowOptions = false;
    vm.keyDownOnCurrentValue = keyDownOnCurrentValue;
    vm.keyDownOnOption = keyDownOnOption;

    function onChanges(changes) {
      if (changes.type && changes.type.currentValue !== null && vm.options) {
        for (var i = 0; i < vm.options.length; i++) {
          if (vm.options[i] === changes.type.currentValue) {
            selectOption(vm.options[i]);
          }
        }
      }
    }

    function onBodyClick() {
      $scope.$apply(function () {
        vm.isShowOptions = !vm.isShowOptions;
      });
    }

    function toggleOptions($event) {
      $event.stopPropagation();
      vm.isShowOptions = !vm.isShowOptions;
    }

    function selectOption(option) {
      vm.selectedValue = option;
      vm.onSelect({value: option});
      $scope.$apply(function () {
          vm.isShowOptions = !vm.isShowOptions;
        });
    }

  function keyDownOnCurrentValue($event) {
    var mainInput = $event.target
    if ($event.which == 13 || $event.keyCode == 13) {
      vm.toggleOptions($event);
      if (vm.isShowOptions) {
        for (var i = 0; i < vm.options.length; i++) {
          if (vm.options[i] === vm.selectedValue) {
          }
        }
      }
    }
  }

  function keyDownOnOption($event) {
    var optionSelected = $event.target
    if ($event.which == 13 || $event.keyCode == 13) {
      selectOption(optionSelected.textContent)
      return false;
    }
    return true;
  }


  }

  return {
    name: "selectBox",
    options: options
  };

});
