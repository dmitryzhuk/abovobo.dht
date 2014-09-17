/**
 * Abovobo DHT Implementation
 *
 * This file is provided under terms and conditions of
 * Eclipse Public License v. 1.0
 * http://www.opensource.org/licenses/eclipse-1.0
 *
 * Developed by Dmitry Zhuk for Abovobo project.
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

    $(document).ready(function () {
        $('#data').nodes({});

        $('h3 button').click(function () {
            $.get('/stop', function () {
                $('.content').wait({'delay': 0, 'action': 'show', 'message': 'Server has stopped'});
            });
        });
    });

}));
