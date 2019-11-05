# The MIT License (MIT)
# Copyright (c) 2019 by the xcube development team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import os.path
import subprocess
import urllib.parse
from typing import Dict, Any

DEFAULT_VIEWER_URL = 'http://xcube-viewer.s3-website.eu-central-1.amazonaws.com'
DEFAULT_SERVER_URL = 'http://ec2-3-123-65-163.eu-central-1.compute.amazonaws.com:8080'
DEFAULT_SERVER_NAME = 'xcube-dcfs Server'


class ViewerServer:
    servers = []

    def __init__(self,
                 *cube_paths,
                 styles: Dict[str, Dict[str, Any]] = None,
                 viewer_url: str = DEFAULT_VIEWER_URL,
                 server_url: str = DEFAULT_SERVER_URL,
                 server_name: str = DEFAULT_SERVER_NAME):
        for cube_path in cube_paths:
            if not os.path.exists(cube_path):
                raise ValueError(f'cube does not exist: {cube_path}')

        style_args = []
        if styles:
            for var_name, style_props in styles.items():
                style_props = dict(style_props)
                vmin = style_props.pop('vmin') if 'vmin' in style_props else 0.0
                vmax = style_props.pop('vmax') if 'vmax' in style_props else 1.0
                cmap = style_props.pop('cmap') if 'cmap' in style_props else 'viridis'
                if style_props:
                    remaining = {var_name: style_props}
                    raise ValueError(f'unrecognized style properties: {remaining}')
                style_args = f'{var_name}=({vmin},{vmax},{cmap!r})'

        args = ['xcube', 'serve', '--address', '0.0.0.0']
        if style_args:
            args.append('--styles')
            args.append(','.join(style_args))

        for cube_path in cube_paths:
            args.append(cube_path)

        print(f'running: {" ".join(args)}')

        self.viewer_url = viewer_url
        self.server_url = server_url
        self.server_name = server_name
        self.process = subprocess.Popen(args,
                                        stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE)

        ViewerServer.servers.append(self)

    def _repr_html_(self):
        return_code = self.process.returncode
        status = 'Running' if return_code is None else f'exited with code {return_code}'
        viewer_url = self.viewer_url + ('/' if not self.server_url.endswith('/') else '')
        server_url = self.server_url
        server_name = self.server_name
        viewer_url_with_server = (f'{viewer_url}'
                                  f'?serverUrl={urllib.parse.quote(server_url)}'
                                  f'&serverName={urllib.parse.quote(server_name)}')
        viewer = f'<a href="{viewer_url_with_server}">Click to open</a>' if return_code is None else 'Not available.'
        return (
            f'<html>'
            f'<table>'
            f'<tr><td>Viewer:</td><td>{viewer}</td></tr>'
            f'<tr><td>Server status:</td><td>{status}</td></tr>'
            f'<tr><td>Server PID:</td><td>{self.process.pid}</td></tr>'
            f'</table>'
            f'</html>'
        )

    def kill(self):
        return self.process.kill()

    @classmethod
    def kill_all(cls):
        for server in cls.servers:
            server.kill()

    @classmethod
    def prune(cls):
        cls.servers = [server for server in cls.servers if server.process.returncode is not None]

    @classmethod
    def set_default_server_url(cls, server_url: str):
        global DEFAULT_SERVER_URL
        DEFAULT_SERVER_URL = server_url
