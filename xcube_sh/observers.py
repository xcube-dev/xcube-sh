import sys

import numpy as np

from .constants import BAND_DATA_ARRAY_NAME


class Observers:

    @classmethod
    def request_dumper(cls):
        """ An observer that dumps each request to stdout."""

        def _dump_request(**kwargs):
            band_name = kwargs['band_name']
            chunk_index = kwargs['chunk_index']
            duration = _format_ms(kwargs['duration'])
            if band_name == BAND_DATA_ARRAY_NAME:
                print(f"Received chunk {chunk_index}, took {duration}")
            else:
                print(f"Received chunk {chunk_index} for band {band_name}: took {duration}")

        return _dump_request

    @classmethod
    def request_collector(cls):
        """ An observer that collects request."""
        return _RequestCollector()


class _RequestCollector:
    def __init__(self):
        self._requests = []

    def __call__(self, **request):
        self._requests.append(request)

    def clear(self):
        self._requests = []

    @property
    def stats(self):
        return _RequestStats(self._requests)


class _RequestStats:
    def __init__(self, requests):
        num_requests = len(requests)
        durations = np.array([request['duration'] for request in requests])
        self.num_requests = num_requests
        if num_requests > 0:
            self.duration_min = durations.min()
            self.duration_max = durations.max()
            self.duration_median = np.median(durations)
            self.duration_mean = durations.mean()
            self.duration_std = durations.std()
            # Another interesting metric is the number of requests
            # for individual chunks (repeated requests) or the number of errors
        else:
            self.duration_min = None
            self.duration_max = None
            self.duration_mean = None
            self.duration_median = None
            self.duration_std = None

    def dump(self, fp=None):
        fp = fp if fp is not None else sys.stdout
        if self.num_requests > 0:
            fp.write(f'Number of requests: {self.num_requests}\n'
                     f'Request duration min: {_format_ms(self.duration_min)}\n'
                     f'Request duration max: {_format_ms(self.duration_max)}\n'
                     f'Request duration median: {_format_ms(self.duration_median)}\n'
                     f'Request duration mean: {_format_ms(self.duration_mean)}\n'
                     f'Request duration std.dev.: {_format_ms(self.duration_std)}\n')
        else:
            fp.write(f'No requests made yet.\n')

    def _repr_html_(self):
        if self.num_requests > 0:
            return (f'<html>'
                    f'<table>'
                    f'<tr><td>Number of requests:</td><td>{self.num_requests}</td></tr>'
                    f'<tr><td>Request duration min:</td><td>{_format_ms(self.duration_min)}</td></tr>'
                    f'<tr><td>Request duration max:</td><td>{_format_ms(self.duration_max)}</td></tr>'
                    f'<tr><td>Request duration median:</td><td>{_format_ms(self.duration_median)}</td></tr>'
                    f'<tr><td>Request duration mean:</td><td>{_format_ms(self.duration_mean)}</td></tr>'
                    f'<tr><td>Request duration std:</td><td>{_format_ms(self.duration_std)}</td></tr>'
                    f'</table>'
                    f'</html>')
        else:
            return (f'<html>'
                    f'<p>No requests made yet.</p>'
                    f'</html>')


def _format_ms(x: float):
    return _format_float(x * 1000) + ' ms'


def _format_float(x: float):
    return '%.2f' % x
