#!/usr/bin/env python3
import pkg_resources


def get_abs_path(relative):
    return pkg_resources.resource_filename(__name__, relative)


with open(get_abs_path('VERSION'), 'r') as _f:
    __version__ = _f.read().strip()
