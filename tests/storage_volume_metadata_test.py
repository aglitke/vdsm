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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301 USA
#
# Refer to the README and COPYING files for full details of the license
#

import time
import uuid

from testlib import VdsmTestCase

from storage import image, sd, volume
from vdsm import define


def get_params(sd_id=None, img_id=None, parent_vol_id=None, size=None,
               vol_format=None, prealloc=None, vol_type=None, disk_type=None,
               description=None, legality=None):
    return dict(
        sd_id=sd_id or str(uuid.uuid4()),
        img_id=img_id or str(uuid.uuid4()),
        parent_vol_id=parent_vol_id or str(uuid.uuid4()),
        size=size or 1024 * define.Mbytes,
        vol_format=vol_format or volume.type2name(volume.RAW_FORMAT),
        prealloc=prealloc or volume.type2name(volume.SPARSE_VOL),
        vol_type=vol_type or volume.type2name(volume.LEAF_VOL),
        disk_type=disk_type or str(image.SYSTEM_DISK_TYPE),
        description=description or "",
        legality=legality or volume.LEGAL_VOL
    )


class VolumeMetadataTests(VdsmTestCase):
    params_to_fields = dict(
        sd_id=volume.DOMAIN,
        img_id=volume.IMAGE,
        parent_vol_id=volume.PUUID,
        size=volume.SIZE,
        vol_format=volume.FORMAT,
        prealloc=volume.TYPE,
        vol_type=volume.VOLTYPE,
        disk_type=volume.DISKTYPE,
        description=volume.DESCRIPTION,
        legality=volume.LEGALITY
    )

    def test_create(self):
        params = get_params()
        md = volume.VolumeMetadata(**params)
        self._compare_params_to_info(params, md.info())

    def test_format(self):
        params = get_params()
        md = volume.VolumeMetadata(**params)
        format_str = md.format()
        format_list = format_str.split('\n')
        self.assertEqual(['EOF', ''], format_list[-2:])
        format_list = format_list[:-2]

        for param_name, info_field in self.params_to_fields.items():
            line = "%s=%s" % (info_field, params[param_name])
            self.assertIn(line, format_list)

        # The following lines are constant
        self.assertIn("%s=0" % volume.MTIME, format_list)
        self.assertIn("%s=" % sd.DMDK_POOLS, format_list)

        # CTIME is set dynamically so check .info for the correct value
        ctime_line = "%s=%i" % (volume.CTIME, md.info()[volume.CTIME])
        self.assertIn(ctime_line, format_list)

    def test_long_description(self):
        params = get_params(description="!" * volume.METADATA_SIZE)
        md = volume.VolumeMetadata(**params)
        self.assertEqual(volume.DESCRIPTION_SIZE, len(md.description))

    def _compare_params_to_info(self, params, info):
        for param_name, info_field in self.params_to_fields.items():
            self.assertEqual(params[param_name], info[info_field])

        # These fields are automatically filled in and have a constant value
        self.assertEqual(0, info[volume.MTIME])
        self.assertEqual("", info[sd.DMDK_POOLS])

        # CTIME is filled in with the current time.  We'll just test that it
        # is close to now.
        self.assertLess(time.time() - info[volume.CTIME], 10)
