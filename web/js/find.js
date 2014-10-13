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
    var name = 'lookup';

    /** Default options for Plugin */
    var defaults = {};

    /** Defines Plugin constructor */
    function Lookup() {}

    /** Plugin prototype definition*/
    Lookup.prototype = {

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
            window.console.log('Initialized Find Plugin');
            this.element = element;
            this.options = $.extend({}, options, defaults);

            var self = this;

            $('button', this.element).attr('disabled','disabled').click(function () {
                self._find($(self.element).children('pre').text(), function (infohash) {
                    /*
                    $('.items', self.element).show().append('<pre>' + infohash + '</pre>');
                    $('input[type="text"]', self.element).val('');
                    $(self.element).children('pre').text('');
                    $('button', self.element).attr('disabled','disabled');
                    */
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

        /**
         * Actually invokes find on backend sending infohash and current node id.
         *
         * @param infohash
         *          An infohash to be found
         * @param success
         *          An optional callback inovoked when announce has succeeded
         * @private
         */
        _find: function (infohash, success) {
            $.get('/node/find/' + infohash + '/' + id, function () {
                if (!!success) {
                    success(infohash);
                }
            });
        }

    };

    $.fn[name] = function (options) {
        return this.each(function () {
            var key = 'plugin_' + name,
                plugin = $.data(this, key);
            if (plugin === undefined) {
                plugin = new Lookup();
                plugin.init(this, options);
                $(this).data(key, plugin);
            }
            plugin.command(options);
        });
    };

}));
