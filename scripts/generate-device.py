#!/usr/bin/env python3
import urllib.request
import pandas
import os
import logging
import json

from zerg.common import log_config, HIGH, LOW

logger = logging.getLogger()
log_config()

FILE = './data.xlsx'


def get_data():
    url = 'http://10.0.38.42/streamdevice-ioc/Redes%20e%20Beaglebones.xlsx'
    urllib.request.urlretrieve(url, FILE)


beagles = {}
master = {}


def uhv():
    global beagles
    global master

    # Data and devices
    SHEET = 'PVs Agilent 4UHV'
    sheet = pandas.read_excel(FILE, sheet_name=SHEET, dtype=str)
    sheet = sheet.replace('nan', '')

    # Beagles
    beagles = {}
    for idx, d in sheet.iterrows():
        beagles[d['IP']] = {
            'app': 'uhv',
            'priority': HIGH,
            'endpoint': 'uhv:' + d['IP'].split('.', maxsplit=2)[-1]
        }
        # @todo: Include another column (IP2) containing the redundant stuff ...

    # Master
    master['uhv'] = {}
    for idx, d in sheet.iterrows():
        endpoint = 'uhv:' + d['IP'].split('.', maxsplit=2)[-1]

        if endpoint not in master['uhv']:
            master['uhv'][endpoint] = {'devices': {}}
        else:
            devices = master['uhv'][endpoint]['devices']
            devices[d['Dispositivo']] = {**d}
            devices[d['Dispositivo']].pop('IP', None)
            devices[d['Dispositivo']].pop('IP2', None)
            devices[d['Dispositivo']].pop('Dispositivo', None)


if __name__ == '__main__':
    uhv()
    with open('beagles.json', 'w+') as _f:
        json.dump(beagles, _f, sort_keys=True, indent=2)

    with open('master.json', 'w+') as _f:
        json.dump(master, _f, sort_keys=True, indent=2)
