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
 * The Connections Main Module.
 *
 * The main module used for supporting the connections functionality.
 **/
define([
  "angular",
  "./app.config",
  "./app.animation",
  "./components/hadoopcluster/hadoopcluster.component",
  "./components/creating/creating.component",
  "./components/success/success.component",
  "./components/selectbox/selectbox.component",
  "./components/controls/controls.component",
  "./components/message/message.component",
  "./components/help/help.component",
  "./directives/ngbodyclick.directive",
  "./service/helper.service",
  "./service/data.service",
  "angular-ui-router",
  "angular-animate"
], function (angular, appConfig, appAnimation, hadoopClusterComponent, creatingComponent, successComponent, selectboxComponent, controlsComponent, messageComponent, helpComponent, bodyClickDirective, helperService, dataService) {
  "use strict";

  var module = {
    name: "hadoop-cluster",
    bootstrap: bootstrap
  };

  activate();

  return module;

  /**
   * Creates angular module with dependencies.
   *
   * @private
   */
  function activate() {
    var deps = ['ui.router', 'ngAnimate'];
    angular.module(module.name, deps)
        .component(hadoopClusterComponent.name, hadoopClusterComponent.options)
        .component(creatingComponent.name, creatingComponent.options)
        .component(successComponent.name, successComponent.options)
        .component(selectboxComponent.name, selectboxComponent.options)
        .component(controlsComponent.name, controlsComponent.options)
        .component(messageComponent.name, messageComponent.options)
        .component(helpComponent.name, helpComponent.options)
        .directive(bodyClickDirective.name, bodyClickDirective.options)
        .service(helperService.name, helperService.factory)
        .service(dataService.name, dataService.factory)
        .animation(appAnimation.class, appAnimation.factory)
        .config(appConfig);
  }

  /**
   * Bootstraps angular module to the DOM element on the page
   * @private
   * @param {DOMElement} element - The DOM element
   */
  function bootstrap(element) {
    angular.element(element).ready(function () {
      angular.bootstrap(element, [module.name], {
        strictDi: true
      });
    });
  }
});
