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
  "angular",
  "./s3.config",
  "./components/step1/step1.component",
  "pentaho/i18n-osgi!pentaho-s3.messages"
], function(angular, config, step1Component, i18n) {
  "use strict";

  function mask(value) {
    if (!value) {
      return i18n.get('S3.Label.NA');
    }
    var password = "";
    for (var i = 0; i < value.length; i++) {
      password += "*";
    }
    return password;
  }

  var module = {
    name: "pentaho-s3-plugin",
    scheme: "s3n",
    label: "Amazon S3",
    summary: [{
      title: i18n.get('Connection.Label.ConnectionDetails'),
      editLink: "s3nstep1",
      mapping: {
        "accessKey": i18n.get('S3.Label.AccessKey'),
        "secretKey": i18n.get('S3.Label.SecretKey'),
        "sessionToken": i18n.get('S3.Label.SessionToken'),
        "region": i18n.get('S3.Label.Region'),
        "credentialsFilePath": i18n.get('S3.Label.Type.CredentialsFile'),
        "profileName": i18n.get('S3.Label.ProfileName')
      },
      filters: {
        "accessKey": mask,
        "secretKey": mask,
        "sessionToken": mask,
        "region": function(value) {
          if (!value) {
            return i18n.get('S3.Label.Default');
          }
          return value;
        },
        credentialsFilePath: function(value) {
          if (!value) {
            return i18n.get('S3.Label.No')
          }
          return value;
        }
      }
    }]
  };
  activate();

  return module;

  /**
   * Creates angular module with dependencies.
   *
   * @private
   */
  function activate() {
    angular.module(module.name, [])
      .component(step1Component.name, step1Component.options)
      .config(config);
  }
});
