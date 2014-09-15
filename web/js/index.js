/**
 * Created by dmitryzhuk on 9/8/14.
 */


/*global define */
(function (factory) {

    'use strict';

    if (typeof define === 'function' && define.amd) {
        // Register as an anonymous AMD module:
        define(['jquery', 'jquery.wait', 'nodes'], factory);
    }

}(function ($) {

    'use strict';

    $('#data').nodes({});

    $('h3 button').click(function () {
        $.get('/stop', function (data) {
            window.alert(1);
            $('.content').wait({'delay': 0, 'action': 'show', 'message': 'Server has stopped'});
        });
    });

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
