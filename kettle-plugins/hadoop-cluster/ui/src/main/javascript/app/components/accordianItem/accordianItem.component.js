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
  'text!./accordianItem.html',
  'pentaho/i18n-osgi!hadoopCluster.messages',
  'css!./accordianItem.css'
], function (template, i18n) {

  'use strict';

  var options = {
    bindings: {
      testCategory: "<"
    },
    controllerAs: "vm",
    template: template,
    controller: accordianItemController
  };

  accordianItemController.$inject = ["$document", "$scope"];

  function accordianItemController($document, $scope) {
    var vm = this;
    vm.$onInit = onInit;
    vm.toggleExpand = toggleExpand;
    vm.getStatusImage = getStatusImage;
    vm.getBodyStatusClass = getBodyStatusClass;
    vm.showCategoryLearnMore = showCategoryLearnMore;

    vm.expandCollapseToggleImage = "img/chevron_down.svg";
    vm.chevronToggleClass = "accordian-chevron-expanded";

    function onInit() {
      vm.learnMoreLabel = i18n.get('accordian.item.learn.more');
    }

    function toggleExpand($event) {
      $event.stopPropagation();
      vm.isExpanded = !vm.isExpanded;
      if (vm.isExpanded) {
        vm.chevronToggleClass = "accordian-chevron-collapsed";
      } else {
        vm.chevronToggleClass = "accordian-chevron-expanded";
      }
    }

    function getBodyStatusClass(status) {
      switch (status) {
        case "Pass":
          return "accordian-body-pass";
        case "Fail":
          return "accordian-body-fail";
        default:
          return "accordian-body-warning";
      }
    }

    function getStatusImage(status) {
      switch (status) {
        case "Pass":
          return "img/success.svg";
        case "Fail":
          return "img/fail.svg";
        default:
          return "img/warning.svg";
      }
    }

    function showCategoryLearnMore(testCategory) {
      return testCategory.tests.length === 0 && testCategory.categoryStatus === "Fail";
    }
  }

  return {
    name: "accordianItem",
    options: options
  };

});
