#
# Copyright 2016 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
#
# Refer to the README and COPYING files for full details of the license
#

# TODO: Adopt the properties framework for managing complex verb parameters

from vdsm import exception
from vdsm.storage import exception as se

from storage import image
from storage import volume


class CreateVolumeInfo(object):
    def __init__(self, params):
        self.sd_id = _required(params, 'sd_id')
        self.img_id = _required(params, 'img_id')
        self.vol_id = _required(params, 'vol_id')
        self.virtual_size = _required(params, 'virtual_size')
        vol_types = [volume.VOLUME_TYPES[vt]
                     for vt in (volume.RAW_FORMAT, volume.COW_FORMAT)]
        self.vol_format = _enum(params, 'vol_format', vol_types)
        self.disk_type = _enum(params, 'disk_type', image.DISK_TYPES.values())
        self.description = params.get('description', '')
        if params.get('parent'):
            self.parent = ParentVolumeInfo(params['parent'])
        else:
            self.parent = None
        self.initial_size = params.get('initial_size', 0)


class ParentVolumeInfo(object):
    def __init__(self, params):
        self.img_id = _required(params, 'img_id')
        self.vol_id = _required(params, 'vol_id')


def _required(params, name):
    if name not in params:
        raise exception.MissingParameter()
    return params[name]


def _enum(params, name, values):
    value = _required(params, name)
    if value not in values:
        raise se.InvalidParameterException(name, value)
    return value
