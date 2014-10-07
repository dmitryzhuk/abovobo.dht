/**
 * Abovobo DHT Implementation
 *
 * This file is provided under terms and conditions of
 * Eclipse Public License v. 1.0
 * http://www.opensource.org/licenses/eclipse-1.0
 *
 * Developed by Dmitry Zhuk for Abovobo project.
 */

/*global define, CryptoJS */
(function (factory) {

    'use strict';

    if (typeof define === 'function' && define.amd) {
        // Register as an anonymous AMD module:
        define(['jquery'], factory);
    }

}(function ($) {

    'use strict';

    var params = new RegExp('[\\?&]id=([^&#]*)').exec(window.location.search),
        id = params === null ? '' : decodeURIComponent(params[1]);

    /** Plugin name */
    var name = 'announce';

    /** Default options for Plugin */
    var defaults = {};

    /** Defines Plugin constructor */
    function Announce() {}

    /** Plugin prototype definition*/
    Announce.prototype = {

        /**
         * Initializes plugin instance. Normally, this method is invoked only once
         * per life cycle of the plugin instance.
         *
         * @param element
         *          HTML element which this plugin instance is bound to
         * @param options
         *          Initialization options
         */
        init: function (element, options) {
            window.console.log('Initialized Announce Plugin');
            this.element = element;
            this.options = $.extend({}, options, defaults);

            var self = this;

            $('button', this.element).attr('disabled','disabled').click(function () {
                self._announce($(self.element).children('pre').text(), function (infohash) {
                    $('.items', self.element).show().append('<pre>' + infohash + '</pre>');
                    $('input[type="text"]', self.element).val('');
                    $(self.element).children('pre').text('');
                    $('button', self.element).attr('disabled','disabled');
                });
            });

            $('input[type="text"]', this.element).keyup(function () {
                var val = this.value.trim();
                if (val.length === 0) {
                    $(this).next().text('');
                    $(this).next().next().attr('disabled','disabled');
                } else {
                    $(this).next().text(CryptoJS.SHA1(val).toString());
                    $(this).next().next().removeAttr('disabled');
                }
            });

        },

        /**
         * Performs particular command on this plugin.
         *
         * @param options
         *          Option describing command to execute.
         */
        command: function (options) {
            //
        },

        _announce: function (infohash, success) {
            var self = this;
            $.get('/node/announce/' + infohash + '/' + id, function () {
                if (!!success) {
                    success(infohash);
                }
                window.setTimeout(function () {
                    self._announce(infohash);
                }, 900000);
            });
        }

    };

    $.fn[name] = function (options) {
        return this.each(function () {
            var key = 'plugin_' + name,
                plugin = $.data(this, key);
            if (plugin === undefined) {
                plugin = new Announce();
                plugin.init(this, options);
                $(this).data(key, plugin);
            }
            plugin.command(options);
        });
    };

}));
