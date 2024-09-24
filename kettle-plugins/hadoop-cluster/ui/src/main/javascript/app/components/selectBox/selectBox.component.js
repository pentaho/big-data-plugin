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
        vm.isShowOptions = false;
      });
    }

    function toggleOptions($event) {
      $event.stopPropagation();
      vm.isShowOptions = !vm.isShowOptions;
    }

    function selectOption(option) {
      vm.selectedValue = option;
      vm.isShowOptions = false;
      vm.onSelect({value: option});
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
