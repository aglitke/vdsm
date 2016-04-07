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

"""
volume_artifacts - construct and deconstruct volumes

In an SDM managed storage domain we will create and remove volumes using
a garbage collection approach rather than persistent tasks and rollback
operations.  Volumes consist of three separate parts: a data area, a
metadata area, and a lease area.  Once created on storage these objects
must be convertible into a Volume with a single atomic operation (ie.
rename a single file).  Conversely, a Volume can be destroyed by
reducing it to its artifacts with a single atomic operation.

VolumeArtifacts is an object to manage the creation and removal of the
three volume artifacts for both block and file based storage.  It also
has methods to manage the conversion of these artifacts to a Volume and
to deconstruct a Volume in order to remove the artifacts from storage.
The three artifacts on storage will not be detected as a volume by other
storage code until they are committed.

Proposed operations for VolumeArtifacts:
 - create: Create the artifacts on storage
 - commit: Convert the artifacts to a volume
 - dismantle: Convert a volume into artifacts
 - clean: Remove the artifacts from storage

Additional methods to identify and garbage collect artifacts will also
be required but the exact interface hasn't settled out yet.

TODO: block based volume
"""

from __future__ import absolute_import

import errno
import logging
import os

from vdsm.storage import exception as se
from vdsm.storage.constants import (
    FILE_VOLUME_PERMISSIONS,
    TEMP_VOL_FILEEXT,
    TEMP_VOL_LVTAG
)

from storage import blockVolume, lvm, volume


class VolumeArtifacts(object):
    log = logging.getLogger('Storage.VolumeArtifacts')

    def __init__(self, sd_manifest, img_id, vol_id):
        """
        Caller must hold the domain lock (paxos lease) and the image resource
        corresponding to self.img_id in exclusive mode.
        """
        self.sd_manifest = sd_manifest
        self.vol_class = self.sd_manifest.getVolumeClass()
        self.img_id = img_id
        self.vol_id = vol_id

    def create(self, size, vol_format, disk_type, desc, parent_vol_id):
        """
        Create a new image and volume artifacts or a new volume inside an
        existing image.  The result is considered as garbage until you invoke
        commit().
        """
        raise NotImplementedError()

    def commit(self):
        """
        Commit volume artifacts created in create(), creating a valid volume.
        On failure, the volume is considered as garbage and can be collected by
        the garbage collector.
        """
        raise NotImplementedError()

    def is_image(self):
        """
        Return True if the image already exists.  We assume that at least one
        volume exists in the image.
        """
        raise NotImplementedError()

    def is_garbage(self):
        """
        Return True if storage contains garbage. This can be a volume that was
        interrupted during creation, a dismantled volume, or volume that was
        interrupted during cleanup.
        """
        raise NotImplementedError()


class FileVolumeArtifacts(VolumeArtifacts):
    """
    A file based volume can be in one of these states:

    MISSING

    - States:
        - no image or volatile directories
    - Operations:
        - is_garbage -> false
        - is_image -> false
        - create artifacts -> change state GARBAGE

    GARBAGE

    - States:
        - volatile image directory
        - image directory containing a volatile metadata file
    - Operations:
        - is_garbage -> true
        - is_image -> true or false
        - clean -> change state to MISSING or VOLUME, GARBAGE if failed
        - commit -> change state to VOLUME, GARBAGE if failed

    VOLUME

    - States:
       - image directory with volume files
    - Operations:
        - is_garbage -> false
        - is_image -> true
        - create new volume -> change state to GARBAGE
        - destroy this volume -> change state to GARBAGE
    """
    log = logging.getLogger('Storage.FileVolumeArtifacts')

    def __init__(self, sd_manifest, img_id, vol_id):
        super(FileVolumeArtifacts, self).__init__(sd_manifest, img_id,
                                                  vol_id)
        self._image_dir = self.sd_manifest.getImagePath(img_id)

    def is_garbage(self):
        volatile_img_dir = self.sd_manifest.getDeletedImagePath(self.img_id)
        if self._oop.fileUtils.pathExists(volatile_img_dir):
            return True

        return self._oop.fileUtils.pathExists(self.meta_volatile_path)

    def is_image(self):
        return self._oop.fileUtils.pathExists(self._image_dir)

    @property
    def _oop(self):
        return self.sd_manifest.oop

    @property
    def artifacts_dir(self):
        # If the artifacts are being added to an existing image we can create
        # them in that image directory.  If the artifacts represent the first
        # volume in a new image then use a new temporary image directory.
        if self.is_image():
            return self._image_dir
        else:
            return self.sd_manifest.getDeletedImagePath(self.img_id)

    @property
    def meta_volatile_path(self):
        return self.meta_path + TEMP_VOL_FILEEXT

    @property
    def meta_path(self):
        vol_path = os.path.join(self.artifacts_dir, self.vol_id)
        return self.vol_class.metaVolumePath(vol_path)

    @property
    def lease_path(self):
        return self.vol_class.leaseVolumePath(self.volume_path)

    @property
    def volume_path(self):
        return os.path.join(self.artifacts_dir, self.vol_id)

    def create(self, size, vol_format, disk_type, desc,
               parent_vol_id=volume.BLANK_UUID):
        """
        Create metadata file artifact, lease file, and volume file on storage.
        """
        # TODO: Support initialsize
        # XXX: Remove these when support is added:
        if vol_format != volume.RAW_FORMAT:
            raise NotImplementedError("Only raw volumes are supported")
        if parent_vol_id != volume.BLANK_UUID:
            raise NotImplementedError("parent_vol_id not supported")

        if self.is_image() and parent_vol_id == volume.BLANK_UUID:
            raise se.InvalidParameterException("parent_vol_id", parent_vol_id)

        prealloc = self._get_volume_preallocation(vol_format)
        self.sd_manifest.validateCreateVolumeParams(
            vol_format, parent_vol_id, preallocate=prealloc)

        if not self.is_image():
            self._create_image_artifact()

        self._create_metadata_artifact(size, vol_format, prealloc, disk_type,
                                       desc, parent_vol_id)
        self._create_lease_file()
        self._create_volume_file(vol_format, size)

    def commit(self):
        try:
            self._oop.os.rename(self.meta_volatile_path, self.meta_path)
        except OSError as e:
            if e.errno == errno.EEXIST:
                raise se.VolumeAlreadyExists("Path %r exists", self.meta_path)
            raise

        # If we created a new image directory, rename it to the correct name
        if not self.is_image():
            self._oop.os.rename(self.artifacts_dir, self._image_dir)

    def _get_volume_preallocation(self, vol_format):
        # File volumes are always sparse regardless of format
        return volume.SPARSE_VOL

    def _create_metadata_artifact(self, size, vol_format, prealloc, disk_type,
                                  desc, parent_vol_id):
        if self._oop.fileUtils.pathExists(self.meta_path):
            raise se.VolumeAlreadyExists("metadata exists: %r" %
                                         self.meta_path)

        if self._oop.fileUtils.pathExists(self.meta_volatile_path):
            raise se.DomainHasGarbage("metadata artifact exists: %r" %
                                      self.meta_volatile_path)

        # Create the metadata artifact.  The metadata file is created with a
        # special extension to prevent these artifacts from being recognized as
        # a volume until FileVolumeArtifacts.commit() is called.
        meta_dict = self.vol_class.new_metadata_dict(
            self.sd_manifest.sdUUID,
            self.img_id,
            parent_vol_id,
            size / volume.BLOCK_SIZE,  # Size is stored as number of blocks
            volume.type2name(vol_format),
            volume.type2name(prealloc),
            volume.type2name(volume.LEAF_VOL),
            disk_type,
            desc,
            volume.LEGAL_VOL)

        data = self.vol_class.formatMetadata(meta_dict)
        self._oop.writeLines(self.meta_volatile_path, data)

    def _create_lease_file(self):
        if self.sd_manifest.hasVolumeLeases():
            meta_id = (self.volume_path,)
            self.vol_class.newVolumeLease(meta_id, self.sd_manifest.sdUUID,
                                          self.vol_id)

    def _create_volume_file(self, vol_format, size):
        trunc_size = size if vol_format == volume.RAW_FORMAT else 0
        self._oop.truncateFile(
            self.volume_path, trunc_size,
            mode=FILE_VOLUME_PERMISSIONS, creatExcl=True)

    def _create_image_artifact(self):
        self.log.debug("Creating image artifact directory: %r",
                       self.artifacts_dir)
        try:
            self._oop.os.mkdir(self.artifacts_dir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

            # We have existing artifacts in the way.  Time to run
            # garbage collection
            raise se.DomainHasGarbage("artifacts directory exists: %r" %
                                      self.artifacts_dir)


class BlockVolumeArtifacts(VolumeArtifacts):
    """
    A block based volume can be in one of these states:

    MISSING

    - States:
    - No logical volume exists
    - Operations:
    - is_garbage -> false
    - is_image -> false
    - create artifacts -> change state GARBAGE

    GARBAGE

    - States:
    - A logical volume with the TEMP_VOL_LVTAG tag exists
    - Operations:
    - is_garbage -> true
    - is_image -> true or false
    - clean -> change state to MISSING
    - commit -> change state to VOLUME

    VOLUME

    - States:
    - A logical volume without the TEMP_VOL_LVTAG tag exists
    - Operations:
    - is_garbage -> false
    - is_image -> true
    - create new volume -> change state to GARBAGE
    - destroy this volume -> change state to GARBAGE
    """
    log = logging.getLogger('Storage.BlockVolumeArtifacts')

    def __init__(self, sd_manifest, img_id, vol_id):
        self.vol_class = sd_manifest.getVolumeClass()
        super(BlockVolumeArtifacts, self).__init__(sd_manifest, img_id,
                                                   vol_id)

    def is_image(self):
        # TODO: Cache this value to avoid repeated expensive queries
        return self.img_id in self.sd_manifest.getAllImages()

    def is_garbage(self):
        try:
            lv = lvm.getLV(self.sd_manifest.sdUUID, self.vol_id)
        except se.LogicalVolumeDoesNotExistError:
            return False
        return TEMP_VOL_LVTAG in lv.tags

    def create(self, size, vol_format, disk_type, desc,
               parent_vol_id=volume.BLANK_UUID):
        # TODO: Support initialsize
        # XXX: Remove these when support is added:
        if vol_format != volume.RAW_FORMAT:
            raise NotImplementedError("Only raw volumes are supported")
        if parent_vol_id != volume.BLANK_UUID:
            raise NotImplementedError("parent_vol_id not supported")

        prealloc = self.get_volume_preallocation(vol_format)
        lv_size = self.vol_class.calculate_volume_alloc_size(
            prealloc, size, None)

        if self.is_image() and parent_vol_id == volume.BLANK_UUID:
            raise se.InvalidParameterException("parent_vol_id", parent_vol_id)

        self.sd_manifest.validateCreateVolumeParams(
            vol_format, parent_vol_id, preallocate=prealloc)

        self._create_lv_artifact(parent_vol_id, lv_size)
        meta_id = self._create_metadata(size, vol_format, prealloc, disk_type,
                                        desc, parent_vol_id)
        self._create_lease(meta_id)

    def commit(self):
        lvm.changeLVTags(self.sd_manifest.sdUUID, self.vol_id, addTags=(),
                         delTags=(TEMP_VOL_LVTAG,))

    def get_volume_preallocation(self, vol_format):
        if vol_format == volume.RAW_FORMAT:
            return volume.PREALLOCATED_VOL
        else:
            return volume.SPARSE_VOL

    def _create_lv_artifact(self, parent_vol_id, lv_size):
        try:
            lvm.getLV(self.sd_manifest.sdUUID, self.vol_id)
        except se.LogicalVolumeDoesNotExistError:
            pass
        else:
            raise se.DomainHasGarbage("Logical volume %s exists" % self.vol_id)

        tags = [TEMP_VOL_LVTAG,
                blockVolume.TAG_PREFIX_PARENT + parent_vol_id,
                blockVolume.TAG_PREFIX_IMAGE + self.img_id]
        lvm.createLV(self.sd_manifest.sdUUID, self.vol_id, lv_size,
                     activate=True, initialTags=tags)

    def _create_metadata(self, size, vol_format, prealloc, disk_type, desc,
                         parent_vol_id):
        size = self.vol_class.get_new_volume_actual_size(
            self.sd_manifest, self.img_id, self.vol_id, vol_format, size)

        with self.sd_manifest.acquireVolumeMetadataSlot(
                self.vol_id, blockVolume.VOLUME_MDNUMBLKS) as mdSlot:
            sd_id = self.sd_manifest.sdUUID
            lvm.changeLVTags(sd_id, self.vol_id,
                             addTags=[blockVolume.TAG_PREFIX_MD + str(mdSlot)])
            meta_id = (sd_id, mdSlot)

        self.vol_class.newMetadata(
            meta_id,
            sd_id,
            self.img_id,
            parent_vol_id,
            size / volume.BLOCK_SIZE,  # Size is stored as number of blocks
            volume.type2name(vol_format),
            volume.type2name(prealloc),
            volume.type2name(volume.LEAF_VOL),
            disk_type,
            desc,
            volume.LEGAL_VOL)
        return meta_id

    def _create_lease(self, meta_id):
        self.vol_class.newVolumeLease(
            meta_id, self.sd_manifest.sdUUID, self.vol_id)
