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

import os
import uuid

from testlib import VdsmTestCase
from storagetestlib import fake_block_env, fake_file_env

from vdsm.storage import exception as se
from vdsm.storage.constants import TEMP_VOL_LVTAG

from storage import image, sd, volume


class ExpectedFailure(Exception):
    pass


BASE_RAW_PARAMS = (1073741824, volume.RAW_FORMAT,
                   image.SYSTEM_DISK_TYPE, 'raw_volume')
BASE_COW_PARAMS = (1073741824, volume.COW_FORMAT,
                   image.SYSTEM_DISK_TYPE, 'cow_volume')


class VolumeArtifactsTestsMixin(object):

    def setUp(self):
        self.img_id = str(uuid.uuid4())
        self.vol_id = str(uuid.uuid4())

    def test_state_missing(self):
        with self.fake_env() as env:
            artifacts = env.sd_manifest.get_volume_artifacts(
                self.img_id, self.vol_id)
            self.assertFalse(artifacts.is_garbage())
            self.assertFalse(artifacts.is_image())
            self.assertRaises(AssertionError,
                              self.validate_artifacts, artifacts, env)

    def test_state_garbage_volatile_image_dir(self):
        with self.fake_env() as env:
            artifacts = env.sd_manifest.get_volume_artifacts(
                self.img_id, self.vol_id)
            artifacts.create(*BASE_RAW_PARAMS)
            self.assertTrue(artifacts.is_garbage())
            self.assertFalse(artifacts.is_image())
            self.validate_artifacts(artifacts, env)

    def test_state_garbage_create_raises(self):
        with self.fake_env() as env:
            artifacts = env.sd_manifest.get_volume_artifacts(
                self.img_id, self.vol_id)
            artifacts.create(*BASE_RAW_PARAMS)
            self.assertRaises(se.DomainHasGarbage, artifacts.create,
                              *BASE_RAW_PARAMS)

    def test_state_image(self):
        with self.fake_env() as env:
            artifacts = env.sd_manifest.get_volume_artifacts(
                self.img_id, self.vol_id)
            artifacts.create(*BASE_RAW_PARAMS)
            artifacts.commit()
            self.assertFalse(artifacts.is_garbage())
            self.assertTrue(artifacts.is_image())

    def test_create_additional_vol_missing_parent_id(self):
        with self.fake_env() as env:
            first = env.sd_manifest.get_volume_artifacts(
                self.img_id, self.vol_id)
            first.create(*BASE_RAW_PARAMS)
            first.commit()
            second = env.sd_manifest.get_volume_artifacts(
                self.img_id, str(uuid.uuid4()))
            self.assertRaises(NotImplementedError,
                              second.create, *BASE_COW_PARAMS)

    def test_create_additional_raw_vol(self):
        with self.fake_env() as env:
            first = env.sd_manifest.get_volume_artifacts(
                self.img_id, self.vol_id)
            first.create(*BASE_RAW_PARAMS)
            first.commit()
            second = env.sd_manifest.get_volume_artifacts(
                self.img_id, str(uuid.uuid4()))
            self.assertRaises(se.InvalidParameterException, second.create,
                              *BASE_RAW_PARAMS)

    def test_new_image_create_and_commit(self):
        with self.fake_env() as env:
            artifacts = env.sd_manifest.get_volume_artifacts(
                self.img_id, self.vol_id)
            size, vol_format, disk_type, desc = BASE_RAW_PARAMS
            artifacts.create(size, vol_format, disk_type, desc)
            artifacts.commit()
            vol = env.sd_manifest.produceVolume(self.img_id, self.vol_id)
            self.assertEqual(volume.type2name(volume.LEAF_VOL),
                             vol.getVolType())
            self.assertEqual(desc, vol.getDescription())
            self.assertEqual(volume.LEGAL_VOL, vol.getLegality())
            self.assertEqual(size / volume.BLOCK_SIZE, vol.getSize())
            self.assertEqual(vol_format, vol.getFormat())
            self.assertEqual(str(disk_type), vol.getDiskType())

    # Artifacts visibility

    def test_getallvolumes(self):
        # Artifacts must not be recognized as volumes until commit is called.
        with self.fake_env() as env:
            self.assertEqual({}, env.sd_manifest.getAllVolumes())
            artifacts = env.sd_manifest.get_volume_artifacts(
                self.img_id, self.vol_id)
            artifacts.create(*BASE_RAW_PARAMS)
            self.assertEqual({}, env.sd_manifest.getAllVolumes())
            artifacts.commit()
            self.assertIn(self.vol_id, env.sd_manifest.getAllVolumes())


class FileVolumeArtifactsTests(VolumeArtifactsTestsMixin, VdsmTestCase):

    def fake_env(self):
        return fake_file_env()

    def test_raw_volume_preallocation(self):
        with self.fake_env() as env:
            artifacts = env.sd_manifest.get_volume_artifacts(
                self.img_id, self.vol_id)
            size, vol_format, disk_type, desc = BASE_RAW_PARAMS
            artifacts.create(size, vol_format, disk_type, desc)
            artifacts.commit()
            vol = env.sd_manifest.produceVolume(self.img_id, self.vol_id)
            self.assertEqual(volume.SPARSE_VOL, vol.getType())

    def test_new_image_create_metadata_failure(self):
        # If we fail before the metadata is created we will have an empty
        # image directory with a garbage collection prefix left behind
        with self.fake_env() as env:
            artifacts = env.sd_manifest.get_volume_artifacts(
                self.img_id, self.vol_id)
            artifacts._create_metadata_artifact = self.failure
            self.assertRaises(ExpectedFailure, artifacts.create,
                              *BASE_RAW_PARAMS)
            self.validate_new_image_path(artifacts)

    def test_new_image_create_lease_failure(self):
        # If we fail before the lease is created we will have a garbage image
        # directory containing a metadata file with the .artifact extension
        with self.fake_env() as env:
            artifacts = env.sd_manifest.get_volume_artifacts(
                self.img_id, self.vol_id)
            artifacts._create_lease_file = self.failure
            self.assertRaises(ExpectedFailure, artifacts.create,
                              *BASE_RAW_PARAMS)
            self.validate_new_image_path(artifacts, has_md=True)

            # We cannot re-create in this state because garbage left behind
            artifacts = env.sd_manifest.get_volume_artifacts(
                self.img_id, self.vol_id)
            self.assertRaises(se.DomainHasGarbage, artifacts.create,
                              *BASE_RAW_PARAMS)

    def test_new_image_create_container_failure(self):
        # If we fail before the container is created we will have a garbage
        # image directory containing artifact metadata and a lease file.
        with self.fake_env() as env:
            artifacts = env.sd_manifest.get_volume_artifacts(
                self.img_id, self.vol_id)
            artifacts._create_volume_file = self.failure
            self.assertRaises(ExpectedFailure, artifacts.create,
                              *BASE_RAW_PARAMS)
            self.validate_new_image_path(artifacts,
                                         has_md=True, has_lease=True)

            # We cannot re-create in this state because garbage left behind
            artifacts = env.sd_manifest.get_volume_artifacts(
                self.img_id, self.vol_id)
            self.assertRaises(se.DomainHasGarbage, artifacts.create,
                              *BASE_RAW_PARAMS)

    def test_garbage_image_dir(self):
        # Creating the an artifact using an existing garbage image directory is
        # not allowed.
        with self.fake_env() as env:
            artifacts = env.sd_manifest.get_volume_artifacts(
                self.img_id, self.vol_id)
            artifacts._create_metadata_artifact = self.failure
            self.assertRaises(ExpectedFailure, artifacts.create,
                              *BASE_RAW_PARAMS)
            artifacts = env.sd_manifest.get_volume_artifacts(
                self.img_id, self.vol_id)
            self.assertRaises(se.DomainHasGarbage, artifacts.create,
                              *BASE_RAW_PARAMS)

    def validate_new_image_path(self, artifacts, has_md=False,
                                has_lease=False, has_volume=False):
        path = artifacts.artifacts_dir
        self.assertTrue(
            os.path.basename(path).startswith(sd.REMOVED_IMAGE_PREFIX))
        self.assertTrue(os.path.exists(path))
        self.assertFalse(os.path.exists(artifacts._image_dir))
        self.assertEqual(has_md, os.path.exists(artifacts.meta_volatile_path))
        self.assertEqual(has_lease, os.path.exists(artifacts.lease_path))
        self.assertEqual(has_volume, os.path.exists(artifacts.volume_path))

    def validate_artifacts(self, artifacts, env):
        self.assertTrue(os.path.exists(artifacts.meta_volatile_path))
        self.assertTrue(os.path.exists(artifacts.volume_path))
        self.assertTrue(os.path.exists(artifacts.lease_path))

    def failure(*args):
        raise ExpectedFailure()

    # Invalid use of artifacts

    def test_new_image_commit_without_create(self):
        with self.fake_env() as env:
            artifacts = env.sd_manifest.get_volume_artifacts(
                    self.img_id, self.vol_id)
            self.assertRaises(OSError, artifacts.commit)

    def test_new_image_commit_twice(self):
        with self.fake_env() as env:
            artifacts = env.sd_manifest.get_volume_artifacts(
                    self.img_id, self.vol_id)
            artifacts.create(*BASE_RAW_PARAMS)
            artifacts.commit()
            self.assertRaises(OSError, artifacts.commit)


class FileVolumeArtifactVisibilityTests(VdsmTestCase):

    def setUp(self):
        self.img_id = str(uuid.uuid4())
        self.vol_id = str(uuid.uuid4())

    def test_getallimages(self):
        # The current behavior of getAllImages is to report garbage image
        # directories (perhaps this should be changed).
        with fake_file_env() as env:
            garbage_img_id = sd.REMOVED_IMAGE_PREFIX + self.img_id
            self.assertEqual(set(), env.sd_manifest.getAllImages())
            artifacts = env.sd_manifest.get_volume_artifacts(
                self.img_id, self.vol_id)
            artifacts.create(*BASE_RAW_PARAMS)
            self.assertEqual({garbage_img_id}, env.sd_manifest.getAllImages())
            artifacts.commit()
            self.assertEqual({self.img_id}, env.sd_manifest.getAllImages())


class BlockVolumeArtifactsTests(VolumeArtifactsTestsMixin, VdsmTestCase):

    def fake_env(self):
        return fake_block_env()

    def test_raw_volume_preallocation(self):
        with self.fake_env() as env:
            artifacts = env.sd_manifest.get_volume_artifacts(
                    self.img_id, self.vol_id)
            size, vol_format, disk_type, desc = BASE_RAW_PARAMS
            artifacts.create(size, vol_format, disk_type, desc)
            artifacts.commit()
            vol = env.sd_manifest.produceVolume(self.img_id, self.vol_id)
            self.assertEqual(volume.PREALLOCATED_VOL, vol.getType())

    def validate_artifacts(self, artifacts, env):
        try:
            lv = env.lvm.getLV(artifacts.sd_manifest.sdUUID, artifacts.vol_id)
        except se.LogicalVolumeDoesNotExistError:
            self.log.debug("LV missing")
            raise AssertionError()

        if TEMP_VOL_LVTAG not in lv.tags:
            self.log.debug("Missing TEMP_VOL_LVTAG")
            raise AssertionError()

        # TODO: Validate lease and metadata allocation

    # Invalid use of artifacts

    def test_commit_without_create(self):
        with self.fake_env() as env:
            artifacts = env.sd_manifest.get_volume_artifacts(
                    self.img_id, self.vol_id)
            self.assertRaises(se.LogicalVolumeReplaceTagError,
                              artifacts.commit)

    def test_commit_twice(self):
        with self.fake_env() as env:
            artifacts = env.sd_manifest.get_volume_artifacts(
                    self.img_id, self.vol_id)
            artifacts.create(*BASE_RAW_PARAMS)
            artifacts.commit()
            artifacts.commit()  # removing nonexistent tags is allowed
