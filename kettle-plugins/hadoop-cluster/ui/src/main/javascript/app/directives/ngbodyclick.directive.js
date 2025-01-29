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
  'angular'
], function (angular) {

  'use strict';

  function bodyclick($document) {
    return {
      restrict: 'A',
      scope: {
        'ngBodyClick': '&'
      },
      link: function (scope, element, attr) {
        angular.element($document).bind('click', function () {
          scope.ngBodyClick();
        });
      }
    };
  }

  return {
    name: "ngBodyClick",
    options: ["$document", bodyclick]
  };
});
