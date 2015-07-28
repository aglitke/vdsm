# Copyright 2015 Red Hat, Inc.
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

import uuid
import os

from testlib import VdsmTestCase, namedTemporaryDir

from storage import sd, image
from storage.sdmprotect import sdm_verb


class ImageManifestTests(VdsmTestCase):
    def test_getimagedir(self):
        with namedTemporaryDir() as tmpdir:
            sduuid = str(uuid.uuid4())
            imguuid = str(uuid.uuid4())
            manifest = image.ImageManifest(tmpdir)
            expected = os.path.join(tmpdir, sduuid, sd.DOMAIN_IMAGES, imguuid)
            self.assertEquals(expected, manifest.getImageDir(sduuid, imguuid))

    @sdm_verb
    def test_create_image_dir(self):
        with namedTemporaryDir() as tmpdir:
            sduuid = str(uuid.uuid4())
            imguuid = str(uuid.uuid4())
            manifest = image.ImageManifest(tmpdir)
            os.makedirs(os.path.join(tmpdir, sduuid, sd.DOMAIN_IMAGES))
            manifest.create_image_dir(sduuid, imguuid)
            self.assertTrue(os.path.exists(manifest.getImageDir(sduuid,
                                                                imguuid)))