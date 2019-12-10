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
  "./components/import/import.component",
  "./components/newEdit/newEdit.component",
  "./components/creating/creating.component",
  "./components/testing/testing.component",
  "./components/status/status.component",
  "./components/testResults/testResults.component",
  "./components/addDriver/addDriver.component",
  "./components/installingDriver/installingDriver.component",
  "./components/driverStatus/driverStatus.component",
  "./components/security/security.component",
  "./components/kerberos/kerberos.component",
  "./components/knox/knox.component",
  "./components/browse/browse.component",
  "./components/selectBox/selectBox.component",
  "./components/multiBrowse/multiBrowse.component",
  "./components/credentials/credentials.component",
  "./components/accordianItem/accordianItem.component",
  "./components/modalDialog/modalDialog.component",
  "./components/controls/controls.component",
  "./components/help/help.component",
  "./directives/ngbodyclick.directive",
  "./directives/fileInput.directive",
  "./service/helper.service",
  "./service/data.service",
  "./service/file.service",
  "angular-ui-router",
  "angular-animate"
], function (angular, appConfig, appAnimation, importComponent, newEditComponent, creatingComponent, testingComponent,
             statusComponent, testResultsComponent, addDriverComponent, installingDriverComponent,
             driverStatusComponent, securityComponent, kerberosComponent, knoxComponent, browseComponent, selectBoxComponent,
             multiBrowseComponent, credentialsComponent, accordianItemComponent, modalDialogComponent,
             controlsComponent, helpComponent, fileInputDirective, bodyClickDirective, helperService, dataService, fileService) {
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
        .component(importComponent.name, importComponent.options)
        .component(newEditComponent.name, newEditComponent.options)
        .component(creatingComponent.name, creatingComponent.options)
        .component(testingComponent.name, testingComponent.options)
        .component(statusComponent.name, statusComponent.options)
        .component(testResultsComponent.name, testResultsComponent.options)
        .component(addDriverComponent.name, addDriverComponent.options)
        .component(installingDriverComponent.name, installingDriverComponent.options)
        .component(driverStatusComponent.name, driverStatusComponent.options)
        .component(securityComponent.name, securityComponent.options)
        .component(kerberosComponent.name, kerberosComponent.options)
        .component(knoxComponent.name, knoxComponent.options)
        .component(browseComponent.name, browseComponent.options)
        .component(selectBoxComponent.name, selectBoxComponent.options)
        .component(multiBrowseComponent.name, multiBrowseComponent.options)
        .component(credentialsComponent.name, credentialsComponent.options)
        .component(accordianItemComponent.name, accordianItemComponent.options)
        .component(modalDialogComponent.name, modalDialogComponent.options)
        .component(controlsComponent.name, controlsComponent.options)
        .component(helpComponent.name, helpComponent.options)
        .directive(bodyClickDirective.name, bodyClickDirective.options)
        .directive(fileInputDirective.name, fileInputDirective.options)
        .service(helperService.name, helperService.factory)
        .service(dataService.name, dataService.factory)
        .service(fileService.name, fileService.factory)
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
