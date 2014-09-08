/**
 * Created by dmitryzhuk on 9/8/14.
 */


/*global define */
(function (factory) {

    'use strict';

    if (typeof define === 'function' && define.amd) {
        // Register as an anonymous AMD module:
        define(['jquery'], factory);
    }

}(function ($) {

    'use strict';

    /** Default options for Plugin */
    var name = 'nodes';

    /** Defines Plugin constructor */
    function Nodes() {}

    /** Plugin prototype definition*/
    Nodes.prototype = {
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
            $.getJSON('/list/0/100', function (data) {

            });
            $('#routers', element);
            window.console.log('Initialized Nodes Plugin');
        },

        /**
         * Performs particular command on this plugin.
         *
         * @param options
         *          Option describing command to execute.
         */
        command: function (options) {
            //
        }

    };

    $.fn[name] = function (options) {
        return this.each(function () {
            var key = 'plugin_' + name,
                plugin = $.data(this, key);
            if (plugin === undefined) {
                plugin = new Nodes();
                plugin.init(this, options);
                $(this).data(key, plugin);
            }
            plugin.command(options);
        });
    };

}));
