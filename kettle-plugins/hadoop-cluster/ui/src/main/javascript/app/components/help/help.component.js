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
