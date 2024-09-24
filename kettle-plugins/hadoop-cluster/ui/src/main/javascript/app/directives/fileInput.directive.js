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
  'angular'
], function (angular) {

  'use strict';

  function fileInput($parse) {
    return {
      restrict: 'A',
      link: function (scope, element, attr) {
        element.bind('change', function () {
          $parse(attr.fileInput).assign(scope, element[0].files);
          scope.$apply();
        });
      }
    };
  }

  return {
    name: "fileInput",
    options: ["$parse", fileInput]
  };
});
