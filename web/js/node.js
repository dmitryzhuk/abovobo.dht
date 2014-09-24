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
        define(['jquery', 'jquery.wait', 'announce'], factory);
    }

}(function ($) {

    'use strict';

    var params = new RegExp('[\\?&]id=([^&#]*)').exec(window.location.search),
        id = params === null ? '' : decodeURIComponent(params[1]);


    /** Plugin name */
    var name = 'node';

    /** Default options for Plugin */
    var defaults = {
        start: 0,
        page: 20
    };

    /** Defines Plugin constructor */
    function Node() {}

    /** Plugin prototype definition*/
    Node.prototype = {

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
            window.console.log('Initialized Node Plugin');

            this.element = element;
            this.options = $.extend({}, options, defaults);

            var self = this;
            $(self.element).wait({'action': 'show', 'message': 'Loading details'});
            $.getJSON('/node/' + id, function (node) {

                $(self.element).wait({'action': 'hide'});

                self.node = node;

                $('#general tbody', self.element).append(
                    '<tr>' +
                    '<td>' + node.lid + '</td>' +
                    '<td>' + node.uid + '</td>' +
                    '<td>' + node.address + '</td>' +
                    '<td>' + node.port + '</td>' +
                    '<td>' + node.table.length + '</td>' +
                    '<td>' + (function (table) {
                        var count = 0;
                        $.each(table, function (index, item) {
                            count += item.nodes.length;
                        });
                        return count;
                    })(node.table) + '</td>' +
                    '<td>' + node.peers.length + '</td>' +
                    '</tr>'
                );

                var table = $('#table tbody', self.element);
                $.each(node.table, function (index, item) {
                    table.append(
                        '<tr>' +
                            '<td colspan="6">' +
                                'Bucket @' + item.start + ' (last seen at ' + new Date(item.seen).toISOString() + ')' +
                            '</td>' +
                        '</tr>'
                    );
                    $.each(item.nodes, function (index, item) {
                        table.append(
                            '<tr>' +
                                '<td>' + item.uid + '</td>' +
                                '<td>' + item.address + '</td>' +
                                '<td>' + item.port + '</td>' +
                                '<td>' + item.failcount + '</td>' +
                                '<td>' +
                                    (item.queried === null ? '<i>never</i>' : new Date(item.queried).toISOString()) +
                                '</td>' +
                                '<td>' +
                                    (item.replied === null ? '<i>never</i>' : new Date(item.replied).toISOString()) +
                                '</td>' +
                            '</tr>'
                        );
                    });
                });

                var peers = $('#peers tbody', self.element);
                $.each(node.peers, function (index, item) {
                    peers.append(
                        '<tr>' +
                            '<td>' + item.infohash + '</td>' +
                            '<td>' + item.address + '</td>' +
                            '<td>' + item.port + '</td>' +
                            '<td>' + new Date(item.announced).toISOString() + '</td>' +
                        '</tr>'
                    );
                });
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
        }

    };

    $.fn[name] = function (options) {
        return this.each(function () {
            var key = 'plugin_' + name,
                plugin = $.data(this, key);
            if (plugin === undefined) {
                plugin = new Node();
                plugin.init(this, options);
                $(this).data(key, plugin);
            }
            plugin.command(options);
        });
    };

    $(document).ready(function () {
        $('.content').node();
        $('#announce').announce();
        $('h3 button').click(function () {
            $.get('/stop', function () {
                $('.content').wait({'delay': 0, 'action': 'show', 'message': 'Server has stopped'});
            });
        });
    });

}));
