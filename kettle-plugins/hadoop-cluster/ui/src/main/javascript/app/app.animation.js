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
 * The animation providers for the connections ui
 */
define(
  [],
  function () {
    'use strict';

    var factoryArray = ["$rootScope", "$state", "$transitions", factory];
    var module = {
      class: ".transition",
      factory: factoryArray
    };

    return module;

    /**
     * The appAnimation factory
     *
     * @param {Object} $rootScope
     * @returns {Object} The callbacks for animation
     */
    function factory($rootScope, $state, $transitions) {
      var transition = $state.params.transition;
      $transitions.onStart({ }, function(trans) {
        if (trans.targetState().params().transition) {
          transition = trans.targetState().params().transition;
        }
      });

      return {
        enter: enter,
        leave: leave
      };

      function enter(element, done) {
        switch (transition) {
          case "slideLeft":
            _slideLeftEnter(element, done);
            break;
          case "slideRight":
            _slideRightEnter(element, done);
            break;
          case "fade":
            _fadeEnter(element, done);
            break;
        }
      }

      function leave(element, done) {
        switch (transition) {
          case "slideLeft":
            _slideLeftLeave(element, done);
            break;
          case "slideRight":
            _slideRightLeave(element, done);
            break;
          case "fade":
            _fadeLeave(element, done);
            break;
        }
      }

      function _slideLeftEnter(element, done) {
        jQuery(element).css('left', '100%');
        jQuery(element).animate({
          left: 0
        }, function () {
          done();
        });
      }

      function _slideRightEnter(element, done) {
        jQuery(element).css('left', '-100%');
        jQuery(element).animate({
          left: 0
        }, function () {
          done();
        });
      }

      function _fadeEnter(element, done) {
        jQuery(element).css("opacity", 0);
        jQuery(element).animate({
          opacity: 1
        }, function () {
          done();
        });
      }

      function _slideLeftLeave(element, done) {
        jQuery(element).css('left', 0);
        jQuery(element).animate({
          left: "-100%"
        }, function () {
          done();
        });
      }

      function _slideRightLeave(element, done) {
        jQuery(element).css('left', 0);
        jQuery(element).animate({
          left: '100%'
        }, function () {
          done();
        });
      }

      function _fadeLeave(element, done) {
        jQuery(element).css("opacity", 1);
        jQuery(element).animate({
          opacity: 0
        }, function () {
          done();
        });
      }
    }
  });
