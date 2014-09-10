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

    /** Plugin name */
    var name = 'nodes';

    /** Default options for Plugin */
    var defaults = {
        start: 0,
        page: 50
    };

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
            window.console.log('Initialized Nodes Plugin');

            this.element = element;
            this.options = $.extend({}, options, defaults);

            this.display(this.options.start, this.options.page);
        },

        /**
         * This function renders collection of nodes starting from given offset.
         *
         * @param offset a point in the node collection to start at.
         * @param count a number of nodes to render.
         */
        display: function (offset, count) {
            var self = this;
            $(self.element).wait({'action': 'show', 'message': 'Loading nodes'});
            $.getJSON('/nodes/list/' + offset + '/' + count, function (data) {
                $('#routers tbody', self.element).empty();
                $.each(data.routers, function (index, node) {
                    self._row($('#routers tbody', self.element), node);
                });
                $('#nodes tbody', self.element).empty();
                $.each(data.nodes, function (index, node) {
                    self._row($('#nodes tbody', self.element), node);
                });
                $(self.element).wait({'action': 'hide'});
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

        _row: function (table, node) {
            return table.append(
                '<tr>' +
                    '<td>' + node.lid + '</td>' +
                    '<td>' + node.uid + '</td>' +
                    '<td>' + node.address + '</td>' +
                    '<td>' + node.port + '</td>' +
                    '<td>' + node.buckets + '</td>' +
                    '<td>' + node.nodes + '</td>' +
                    '<td>' + node.peers + '</td>' +
                '</tr>'
            );
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
