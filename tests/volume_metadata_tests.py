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
from testlib import permutations, expandPermutations
from monkeypatch import MonkeyPatchScope
from storagetestlib import make_blocksd_manifest, make_vg,  make_file_volume, \
    make_filesd_manifest, spoof_domain_metadata
from storagefakelib import FakeLVM

from storage import sd, blockSD, fileSD, volume, blockVolume, fileVolume
from storage import storage_exception as se


VOLSIZE = 1048576


class VolumeMetadataTests(VdsmTestCase):
    def test_properties(self):
        with namedTemporaryDir() as tmpdir:
            vmeta = self.make_metadata(tmpdir)
            self.assertEquals(tmpdir, vmeta.repoPath)

    def test_getvolumepath_before_validate(self):
        with namedTemporaryDir() as tmpdir:
            vmeta = self.make_metadata(tmpdir)
            self.assertRaises(se.VolumeAccessError, vmeta.getVolumePath)

    def make_metadata(self, tmpdir, sduuid=None, imguuid=None, voluuid=None):
        return _make_metadata(volume.VolumeMetadata,
                              tmpdir, sduuid, imguuid, voluuid)


class BlockVolumeMetadataTests(VdsmTestCase):
    def test_validate(self):
        with namedTemporaryDir() as tmpdir:
            lvm = FakeLVM(tmpdir)
            with MonkeyPatchScope([(blockSD, 'lvm', lvm),
                                   (blockVolume, 'lvm', lvm)]):
                manifest = make_blocksd_manifest(tmpdir=tmpdir)
                vg_name = make_vg(lvm, manifest)
                lv = str(uuid.uuid4())
                lvm.createLV(vg_name, lv, VOLSIZE)
                imguuid = str(uuid.uuid4())
                vmeta = blockVolume.BlockVolumeMetadata(tmpdir, vg_name,
                                                        imguuid, lv)
                vmeta.validate()
                expected = lvm.lvPath(vg_name, lv)
                link_path = os.readlink(vmeta.getVolumePath())
                self.assertEquals(expected, link_path)

    def test_validate_imagepath_missing(self):
        with namedTemporaryDir() as tmpdir:
            lvm = FakeLVM(tmpdir)
            with MonkeyPatchScope([(blockSD, 'lvm', lvm),
                                   (blockVolume, 'lvm', lvm)]):
                manifest = make_blocksd_manifest(tmpdir=tmpdir)
                vg_name = make_vg(lvm, manifest)
                voluuid = str(uuid.uuid4())
                imguuid = str(uuid.uuid4())
                vmeta = blockVolume.BlockVolumeMetadata(tmpdir, vg_name,
                                                        imguuid, voluuid)
                vmeta.validateImagePath()
                self.assertTrue(os.path.exists(vmeta.imagePath))

    def make_metadata(self, tmpdir, sduuid=None, imguuid=None, voluuid=None):
        return _make_metadata(blockVolume.BlockVolumeMetadata,
                              tmpdir, sduuid, imguuid, voluuid)


@expandPermutations
class FileVolumeMetadataTests(VdsmTestCase):
    def test_validate(self):
        with namedTemporaryDir() as tmpdir:
            manifest = make_filesd_manifest(tmpdir)
            vmeta = self.make_metadata(tmpdir, manifest.sdUUID)
            make_file_volume(manifest.domaindir, VOLSIZE,
                             vmeta.imgUUID, vmeta.volUUID)

            spoof_domain_metadata(manifest, 3, sd.DATA_DOMAIN)
            vmeta.validate()
            expected = os.path.join(manifest.domaindir, sd.DOMAIN_IMAGES,
                                    vmeta.imgUUID, vmeta.volUUID)
            self.assertEquals(expected, vmeta.getVolumePath())

    def test_validate_imagedir_missing(self):
        with namedTemporaryDir() as tmpdir:
            manifest = make_filesd_manifest(tmpdir)
            vmeta = self.make_metadata(tmpdir, manifest.sdUUID)

            self.assertRaises(se.ImagePathError, vmeta.validate)

    def test_validate_imagedir_no_permission(self):
        with namedTemporaryDir() as tmpdir:
            manifest = make_filesd_manifest(tmpdir)
            vmeta = self.make_metadata(tmpdir, manifest.sdUUID)
            make_file_volume(manifest.domaindir, VOLSIZE,
                             vmeta.imgUUID, vmeta.volUUID)
            img_path = os.path.join(manifest.domaindir, sd.DOMAIN_IMAGES,
                                    vmeta.imgUUID)
            os.chmod(img_path, 0333)
            try:
                self.assertRaises(se.ImagePathError, vmeta.validate)
            finally:
                os.chmod(img_path, 0777)

    def test_validate_volume_missing(self):
        with namedTemporaryDir() as tmpdir:
            manifest = make_filesd_manifest(tmpdir)
            vmeta = self.make_metadata(tmpdir, manifest.sdUUID)
            make_file_volume(manifest.domaindir, VOLSIZE,
                             vmeta.imgUUID, vmeta.volUUID)
            vol_path = os.path.join(manifest.domaindir, sd.DOMAIN_IMAGES,
                                    vmeta.imgUUID, vmeta.volUUID)
            os.unlink(vol_path)
            self.assertRaises(se.VolumeDoesNotExist, vmeta.validate)

    @permutations([[sd.ISO_DOMAIN], [sd.DATA_DOMAIN]])
    def test_validate_no_metadata_file(self, domain_class):
        with namedTemporaryDir() as tmpdir:
            manifest = make_filesd_manifest(tmpdir)
            vmeta = self.make_metadata(tmpdir, manifest.sdUUID)
            make_file_volume(manifest.domaindir, VOLSIZE,
                             vmeta.imgUUID, vmeta.volUUID)

            spoof_domain_metadata(manifest, 3, domain_class)
            vol_path = os.path.join(manifest.domaindir, sd.DOMAIN_IMAGES,
                                    vmeta.imgUUID, vmeta.volUUID)
            meta_path = vmeta._getMetaVolumePath(vol_path)
            os.unlink(meta_path)
            if domain_class == sd.DATA_DOMAIN:
                self.assertRaises(se.VolumeDoesNotExist, vmeta.validate)
            else:
                vmeta.validate()
                self.assertEquals(vol_path, vmeta.getVolumePath())

    def make_metadata(self, tmpdir, sduuid=None, imguuid=None, voluuid=None):
        return _make_metadata(fileVolume.FileVolumeMetadata,
                              tmpdir, sduuid, imguuid, voluuid)


def _make_metadata(mdclass, tmpdir, sduuid, imguuid, voluuid):
    sduuid = sduuid or str(uuid.uuid4())
    imguuid = imguuid or str(uuid.uuid4())
    voluuid = voluuid or str(uuid.uuid4())
    return mdclass(tmpdir, sduuid, imguuid, voluuid)
