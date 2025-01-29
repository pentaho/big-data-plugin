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


/**
 * Defines the config for the connections UI
 */
define([], function () {
  'use strict';

  config.$inject = ['$stateProvider', '$urlRouterProvider'];

  /**
   * The config for the file open save app
   *
   * @param {Object} $stateProvider - Controls the state of the app
   */
  function config($stateProvider, $urlRouterProvider) {
    $stateProvider
    .state('import', {
      url: "/import",
      template: "<import></import>",
      params: {
        data: null,
        transition: null
      }
    })
    .state('new-edit', {
      url: "/new-edit",
      template: "<new-edit></new-edit>",
      params: {
        data: null,
        transition: null
      }
    })
    .state('creating', {
      url: "/creating",
      template: "<creating></creating>",
      params: {
        data: null,
        transition: null
      }
    })
    .state('testing', {
      url: "/testing",
      template: "<testing></testing>",
      params: {
        data: null,
        transition: null
      }
    })
    .state('status', {
      url: "/status",
      template: "<status></status>",
      params: {
        data: null,
        transition: null
      }
    })
    .state('test-results', {
      url: "/test-results",
      template: "<test-results></test-results>",
      params: {
        data: null,
        transition: null
      }
    })
    .state('add-driver', {
      url: "/add-driver",
      template: "<add-driver></add-driver>",
      params: {
        data: null,
        transition: null
      }
    })
    .state('installing-driver', {
      url: "/installing-driver",
      template: "<installing-driver></installing-driver>",
      params: {
        data: null,
        transition: null
      }
    }).state('driver-status', {
      url: "/driver-status",
      template: "<driver-status></driver-status>",
      params: {
        data: null,
        transition: null
      }
    }).state('security', {
      url: "/security",
      template: "<security></security>",
      params: {
        data: null,
        transition: null
      }
    }).state('kerberos', {
      url: "/kerberos",
      template: "<kerberos></kerberos>",
      params: {
        data: null,
        transition: null
      }
    }).state('knox', {
      url: "/knox",
      template: "<knox></knox>",
      params: {
        data: null,
        transition: null
      }
    });
    $urlRouterProvider.otherwise("/import");
  }

  return config;
});
