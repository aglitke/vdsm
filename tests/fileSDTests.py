#
# Copyright 2014 Red Hat, Inc.
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

from __future__ import print_function
import fnmatch
import os
import time
import uuid

from testlib import VdsmTestCase as TestCaseBase

from storage import fileSD
from storage import sd


class TestingFileSDManifest(fileSD.FileStorageDomainManifest):
    def __init__(self, uuid, mountpoint, oop):
        self._uuid = uuid
        self._mountpoint = mountpoint
        self._oop = oop

    @property
    def sdUUID(self):
        return self._uuid

    @property
    def mountpoint(self):
        return self._mountpoint

    @property
    def oop(self):
        return self._oop


class FakeGlob(object):

    def __init__(self, files):
        self.files = files

    def glob(self, pattern):
        return fnmatch.filter(self.files, pattern)


class FakeOOP(object):

    def __init__(self, glob=None):
        self.glob = glob


class GetAllVolumesTests(TestCaseBase):

    MOUNTPOINT = "/rhev/data-center/%s" % uuid.uuid4()
    SD_UUID = str(uuid.uuid4())
    IMAGES_DIR = os.path.join(MOUNTPOINT, SD_UUID, sd.DOMAIN_IMAGES)

    def test_no_volumes(self):
        oop = FakeOOP(FakeGlob([]))
        dom = TestingFileSDManifest(self.SD_UUID, self.MOUNTPOINT, oop)
        res = dom.getAllVolumes()
        self.assertEqual(res, {})

    def test_no_templates(self):
        oop = FakeOOP(FakeGlob([
            os.path.join(self.IMAGES_DIR, "image-1", "volume-1.meta"),
            os.path.join(self.IMAGES_DIR, "image-1", "volume-2.meta"),
            os.path.join(self.IMAGES_DIR, "image-1", "volume-3.meta"),
            os.path.join(self.IMAGES_DIR, "image-2", "volume-4.meta"),
            os.path.join(self.IMAGES_DIR, "image-2", "volume-5.meta"),
            os.path.join(self.IMAGES_DIR, "image-3", "volume-6.meta"),
        ]))
        dom = TestingFileSDManifest(self.SD_UUID, self.MOUNTPOINT, oop)
        res = dom.getAllVolumes()

        # These volumes should have parent uuid, but the implementation does
        # not read the meta data files, so this info is not available (None).
        self.assertEqual(res, {
            "volume-1": (("image-1",), None),
            "volume-2": (("image-1",), None),
            "volume-3": (("image-1",), None),
            "volume-4": (("image-2",), None),
            "volume-5": (("image-2",), None),
            "volume-6": (("image-3",), None),
        })

    def test_with_template(self):
        oop = FakeOOP(FakeGlob([
            os.path.join(self.IMAGES_DIR, "template-1", "volume-1.meta"),
            os.path.join(self.IMAGES_DIR, "image-1", "volume-1.meta"),
            os.path.join(self.IMAGES_DIR, "image-1", "volume-2.meta"),
            os.path.join(self.IMAGES_DIR, "image-1", "volume-3.meta"),
            os.path.join(self.IMAGES_DIR, "image-2", "volume-1.meta"),
            os.path.join(self.IMAGES_DIR, "image-2", "volume-4.meta"),
            os.path.join(self.IMAGES_DIR, "image-3", "volume-5.meta"),
        ]))
        dom = TestingFileSDManifest(self.SD_UUID, self.MOUNTPOINT, oop)
        res = dom.getAllVolumes()

        self.assertEqual(len(res), 5)

        # The template image must be first - we have code assuming this.
        self.assertEqual(res["volume-1"].imgs[0], "template-1")

        # The rest of the images have random order.
        self.assertEqual(sorted(res["volume-1"].imgs[1:]),
                         ["image-1", "image-2"])

        # For template volumes we have parent info.
        self.assertEqual(res["volume-1"].parent, sd.BLANK_UUID)

        self.assertEqual(res["volume-2"], (("image-1",), None))
        self.assertEqual(res["volume-3"], (("image-1",), None))
        self.assertEqual(res["volume-4"], (("image-2",), None))
        self.assertEqual(res["volume-5"], (("image-3",), None))

    def test_scale(self):
        # For this test we want real world strings
        images_count = 5000
        template_image_uuid = str(uuid.uuid4())
        template_volume_uuid = str(uuid.uuid4())

        files = []
        template_volume = os.path.join(self.IMAGES_DIR, template_image_uuid,
                                       template_volume_uuid + ".meta")
        files.append(template_volume)

        for i in range(images_count):
            image_uuid = str(uuid.uuid4())
            volume_uuid = str(uuid.uuid4())
            template_volume = os.path.join(self.IMAGES_DIR, image_uuid,
                                           template_volume_uuid + ".meta")
            files.append(template_volume)
            new_volume = os.path.join(self.IMAGES_DIR, image_uuid,
                                      volume_uuid + ".meta")
            files.append(new_volume)

        oop = FakeOOP(FakeGlob(files))
        dom = TestingFileSDManifest(self.SD_UUID, self.MOUNTPOINT, oop)

        start = time.time()
        dom.getAllVolumes()
        elapsed = time.time() - start
        print("%f seconds" % elapsed)

        # This takes 0.065 seconds on my laptop, 1 second should be enough even
        # on overloaded jenkins slave.
        self.assertTrue(elapsed < 1.0, "Elapsed time: %f seconds" % elapsed)
