/**
 * Created by dmitryzhuk on 9/8/14.
 */


/*global define */
(function (factory) {

    'use strict';

    if (typeof define === 'function' && define.amd) {
        // Register as an anonymous AMD module:
        define(['jquery', 'nodes'], factory);
    }

}(function ($) {

    'use strict';

    $('#data').nodes({});

    /*
    $.fn[name] = function (options) {
        return this.each(function () {
            var key = 'plugin_' + name,
                plugin = $.data(this, key);
            if (plugin === undefined) {
                plugin = new Uploader();
                plugin.init(this, options);
                $(this).data(key, plugin);
            }
            plugin.command(options);
        });
    };
    */

}));
