#!/usr/bin/python

from vdsm.netinfo import DUMMY_BRIDGE

# Constants for hook's API
PROVIDER_TYPE_KEY = 'provider_type'
OPENSTACK_NET_PROVIDER_TYPE = 'OPENSTACK_NETWORK'
VNIC_ID_KEY = 'vnic_id'
PLUGIN_TYPE_KEY = 'plugin_type'
PT_BRIDGE = 'LINUX_BRIDGE'
PT_OVS = 'OPEN_VSWITCH'

# The maximum device name length in Linux
DEV_MAX_LENGTH = 14

# Make pyflakes happy
DUMMY_BRIDGE
