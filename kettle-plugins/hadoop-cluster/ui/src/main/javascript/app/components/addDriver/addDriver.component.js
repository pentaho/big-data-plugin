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
    'text!./addDriver.html',
    'pentaho/i18n-osgi!hadoopCluster.messages'
  ], function (template, i18n) {

    'use strict';

    var options = {
      bindings: {},
      controllerAs: "vm",
      template: template,
      controller: addDriverController
    };

    addDriverController.$inject = ["$state", "fileService"];

    function addDriverController($state, fileService) {
      var vm = this;
      vm.$onInit = onInit;

      function onInit() {

        setDialogTitle(i18n.get('hadoop.cluster.title'));

        vm.header = i18n.get('add.driver.header');
        vm.driverSupportMatrixLinkText = i18n.get('add.driver.support.matrix.line.text');
        vm.fileLabel = i18n.get('add.driver.file.label');
        vm.helpLink = i18n.get('add.driver.help');

        vm.data = {
          type: "driver"
        };

        vm.buttons = getButtons();
      }

      function setDialogTitle(title) {
        try {
          setTitle(title);
        } catch (e) {
          console.log(title);
        }
      }

      function getButtons() {
        return [
          {
            label: i18n.get('controls.next.label'),
            class: "primary",
            isDisabled: function () {
              return !vm.driverFile;
            },
            position: "right",
            onClick: function () {

              //UI-router doesn't work to pass files between states, use fileservice to store the file(s), they are later
              //retrieved by the helperService before passing the request to the server.
              fileService.setFiles(vm.driverFile);

              $state.go('installing-driver', {data: vm.data, transition: "slideLeft"});
            }
          },
          {
            label: i18n.get('controls.cancel.label'),
            class: "primary",
            position: "right",
            onClick: function () {
              close();
            }
          }];
      }
    }

    return {
      name: "addDriver",
      options: options
    };

  }
);
