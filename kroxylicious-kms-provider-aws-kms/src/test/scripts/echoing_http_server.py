#!/usr/bin/env python3
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# An HTTP server that echos headers sent by the client back to the response.

import http.server as SimpleHTTPServer
from socketserver import TCPServer
import sys
import json
import dateutil.parser

try:
    port = int(sys.argv[1])
except:
    port = 8080

class GetHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):

    interest = {"Host", "Authorization", "X-Amz-Date"}
    def do_GET(self):
        self.handle_bye()
        self.echo_response()

    def do_POST(self):
        self.handle_bye()
        self.echo_response()

    def echo_response(self):
        headers = {i[0]: [i[1]] for i in self.headers.items() if i[0] in self.interest}
        request_time = int(dateutil.parser.isoparse(headers["X-Amz-Date"][0]).timestamp())
        resp = {"requestTime": request_time,
                "expectedHeaders": headers}
        self.send_response(200)
        self.end_headers()
        self.wfile.write(json.dumps(resp).encode('utf-8'))

    def log_request(self, code='-', size='-'):
        """request logging is not desired"""
        pass

    def handle_bye(self):
        if self.path == '/bye':
            self.server.socket.close()
            sys.exit(0)


TCPServer.allow_reuse_address = True
httpd = TCPServer(('', port), GetHandler)

httpd.serve_forever()
