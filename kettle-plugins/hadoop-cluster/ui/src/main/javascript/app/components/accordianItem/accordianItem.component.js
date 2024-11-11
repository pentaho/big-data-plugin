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
