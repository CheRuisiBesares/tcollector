#!/usr/bin/python
#
# rabbitMQ.py -- a RabbitMQ collector for tcollector/OpenTSDB
# Copyright (C) 2013  The tcollector Authors.
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version.  This program is distributed in the hope that it
# will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
# General Public License for more details.  You should have received a copy
# of the GNU Lesser General Public License along with this program.  If not,
# see <http://www.gnu.org/licenses/>.

import urllib
import json
import sys
import time
import httplib

from urlparse import urlparse
from base64 import b64encode

try:
    from collectors.lib import utils
except ImportError:
    utils = None  # This is handled gracefully in main()

RABBIT_API_URL = "http://{host}:{port}/api/"
INTERVAL = 10

NODE_STATS = [
    'disk_free', 
    'disk_free_limit',
    'fd_total',
    'fd_used',
    'mem_limit', 
    'mem_used',
    'proc_total',
    'proc_used',
    'processors',
    'run_queue',
    'sockets_total', 
    'sockets_used'
    ]

MESSAGE_STATS = [
    'ack',
    'publish',
    'publish_in',
    'publish_out',
    'confirm',
    'deliver',
    'deliver_noack',
    'get',
    'get_noack',
    'deliver_get',
    'redeliver',
    'return'
    ]

MESSAGE_DETAIL = [
    'avg',
    'avg_rate',
    'rate',
    'sample'
    ]

QUEUE_MESSAGE_STATS = [
    'messages', 
    'messages_ready', 
    'messages_unacknowledged'
    ]

QUEUE_STATS = [
    'memory',
    'messages',
    'consumers'
]


PLUGIN_CONFIG = {
    'username': 'guest',
    'password': 'guest',
    'host': 'localhost',
    'port': 15672,
    'realm': 'RabbitMQ Management'
}

class RabbitCollector:
    def __init__(self):
        self.connection = None
        self.auth_headers = { 
            'Authorization' : 'Basic %s' % 
                b64encode(
                    b"%s:%s" %
                    (
                        PLUGIN_CONFIG['username'],
                        PLUGIN_CONFIG['password']
                    )
                ).decode("ascii"),
            "WWW-Authenticate": "Basic realm=\"%s\"" %
                PLUGIN_CONFIG['realm']
        }
        self.url = urlparse(
            RABBIT_API_URL.format(
                host=PLUGIN_CONFIG['host'],
                port=PLUGIN_CONFIG['port']
            )
        )

    def get_info(self, url):
        self.connection.request(
            'GET', 
            "%s/%s" % 
                (
                    self.url.path,
                    url
                ),
            "",
            self.auth_headers
            )
        info = self.connection.getresponse()
        return json.load(info)
    
    def print_metric(self, name, value):
        print "rabbitmq.%s %s %s" % (name, int(time.time()), value)
    
    def unpack_node_stats(self, node):
        for key in NODE_STATS:
            self.print_metric(key, node[key])
    
    def unpack_message_stats(self, data, vhost, plugin, plugin_instance):
        if not data:
            return
    
        for key in MESSAGE_STATS:
            if key in data:
                self.print_metric(
                    '%s.%s.%s.%s' % 
                    (
                        vhost['name'].replace('/', 'default_vhost'),
                        plugin,
                        plugin_instance,
                        key
                    ),
                    data[key]
                )
    
    def unpack_queue_metrics(self, queue, vhost):
        for name in QUEUE_STATS:
            self.print_metric(
                '%s.queues.%s.%s' % 
                (
                    vhost['name'].replace('/', 'default_vhost'),
                    queue['name'],
                    name
                ), 
                queue[name]
            )
    
        for name in QUEUE_MESSAGE_STATS:
            self.print_metric(
                '%s.queues.%s.%s' % 
                (
                    vhost['name'].replace('/', 'default_vhost'),
                    queue['name'],
                    name
                ), 
                queue[name]
            )
    
            details = queue.get("%s_details" % name, None)
            if not details:
                continue
            for detail in MESSAGE_DETAIL:
                if detail in details:
                    self.print_metric(
                        '%s.queues.%s.%s.%s' % 
                        (
                            vhost['name'].replace('/', 'default_vhost'),
                            queue['name'],
                            name,
                            detail
                        ), 
                        details[detail]
                    )
    
        self.unpack_message_stats(
            queue.get('message_stats', None),
            vhost,
            'queues',
            queue['name']
        )
    
    def get_metrics(self):

        self.connection  = httplib.HTTPConnection(self.url.netloc)
    
        #First get all the nodes
        for node in self.get_info("/nodes"):
            self.unpack_node_stats(node)
    
        for vhost in self.get_info("/vhosts"):
    
            vhost_name = urllib.quote(vhost['name'], '')
    
            for queue in self.get_info("/queues/%s" % vhost_name):
                queue_name = urllib.quote(queue['name'], '')
                queue_data = self.get_info("/queues/%s/%s" % (vhost_name,
                                                            queue_name))
                if queue_data is not None:
                    self.unpack_queue_metrics(queue_data, vhost)
    
            for exchange in self.get_info("/exchanges/%s" % vhost_name):
                exchange_name = urllib.quote(exchange['name'], '')
                if exchange_name:
                    exchange_data = self.get_info(
                        "/exchanges/%s/%s" %
                        (
                            vhost_name,
                            exchange_name
                        )
                    )
                    self.unpack_message_stats(exchange_data, vhost, 'exchanges', exchange_name)

        self.connection.close()


def main():
    if utils is not None:
        utils.drop_privileges()
    while True:
        RabbitCollector().get_metrics()
        sys.stdout.flush()
        time.sleep(INTERVAL)

if __name__ == '__main__':
    sys.exit(main())
