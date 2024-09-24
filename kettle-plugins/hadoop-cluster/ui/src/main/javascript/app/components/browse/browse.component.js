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
