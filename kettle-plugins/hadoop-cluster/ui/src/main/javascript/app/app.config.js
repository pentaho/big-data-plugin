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
    });
    $urlRouterProvider.otherwise("/import");
  }

  return config;
});
