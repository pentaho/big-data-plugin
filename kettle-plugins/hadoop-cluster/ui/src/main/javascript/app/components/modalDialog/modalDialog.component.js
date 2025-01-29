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
  'text!./modalDialog.html',
  'pentaho/i18n-osgi!hadoopCluster.messages',
  'css!./modalDialog.css'
], function (template) {

  'use strict';

  var options = {
    bindings: {
      dialogTitle: "<",
      message: "<",
      buttons: "<"
    },
    controllerAs: "vm",
    template: template,
    controller: modalDialogController
  };

  modalDialogController.$inject = [];

  function modalDialogController() {
  }

  return {
    name: "modalDialog",
    options: options
  };

});
