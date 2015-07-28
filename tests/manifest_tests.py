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

import os
import uuid
from contextlib import contextmanager

from testlib import VdsmTestCase, namedTemporaryDir, make_file
from monkeypatch import MonkeyPatchScope
from storagefakelib import FakeLVM
from storagetestlib import (make_filesd_manifest, make_blocksd_manifest,
                            make_file_volume, make_vg, get_random_devices,
                            FakeMetadata, get_uuid_list, create_volume_tree,
                            create_lv_tree)

from storage import sd, blockSD, fileVolume, blockVolume, multipath
from storage import storage_exception as se

MB = 1 << 20
GB = 1 << 30
VOLSIZE = 1 * MB
STORAGE_REPO = '/rhev/data-center'


class FileManifestTests(VdsmTestCase):

    def test_getreaddelay(self):
        with namedTemporaryDir() as tmpdir:
            manifest = make_filesd_manifest(tmpdir)
            self.assertIsInstance(manifest.getReadDelay(), float)

    def test_getvsize(self):
        with namedTemporaryDir() as tmpdir:
            manifest = make_filesd_manifest(tmpdir)
            imguuid, voluuid = make_file_volume(manifest.domaindir, VOLSIZE)
            self.assertEqual(VOLSIZE, manifest.getVSize(imguuid, voluuid))

    def test_getvallocsize(self):
        with namedTemporaryDir() as tmpdir:
            manifest = make_filesd_manifest(tmpdir)
            imguuid, voluuid = make_file_volume(manifest.domaindir, VOLSIZE)
            self.assertEqual(0, manifest.getVAllocSize(imguuid, voluuid))

    def test_getisodomainimagesdir(self):
        with namedTemporaryDir() as tmpdir:
            manifest = make_filesd_manifest(tmpdir)
            isopath = os.path.join(manifest.domaindir, sd.DOMAIN_IMAGES,
                                   sd.ISO_IMAGE_UUID)
            self.assertEquals(isopath, manifest.getIsoDomainImagesDir())

    def test_getmdpath(self):
        with namedTemporaryDir() as tmpdir:
            manifest = make_filesd_manifest(tmpdir)
            mdpath = os.path.join(manifest.domaindir, sd.DOMAIN_META_DATA)
            self.assertEquals(mdpath, manifest.getMDPath())

    def test_getmetaparam(self):
        with namedTemporaryDir() as tmpdir:
            metadata = {sd.DMDK_VERSION: 3}
            manifest = make_filesd_manifest(tmpdir, metadata)
            metadata[sd.DMDK_SDUUID] = manifest.sdUUID
            self.assertEquals(manifest.sdUUID,
                              manifest.getMetaParam(sd.DMDK_SDUUID))

    def test_metadata(self):
        with namedTemporaryDir() as tmpdir:
            metadata = FakeMetadata()
            metadata[sd.DMDK_ROLE] = sd.REGULAR_DOMAIN
            metadata[sd.DMDK_CLASS] = sd.DATA_DOMAIN
            metadata[sd.DMDK_TYPE] = sd.LOCALFS_DOMAIN
            metadata[sd.DMDK_VERSION] = 3
            pooluuid = str(uuid.uuid4())
            metadata[sd.DMDK_POOLS] = [pooluuid]

            manifest = make_filesd_manifest(tmpdir, metadata)
            self.assertEquals(sd.REGULAR_DOMAIN, manifest.getDomainRole())
            self.assertEquals(sd.DATA_DOMAIN, manifest.getDomainClass())
            self.assertTrue(manifest.isData())
            self.assertFalse(manifest.isBackup())
            self.assertEquals(sd.LOCALFS_DOMAIN, manifest.getStorageType())
            self.assertEquals(3, manifest.getVersion())
            self.assertEquals('3', manifest.getFormat())
            self.assertEquals(os.path.join(STORAGE_REPO, pooluuid),
                              manifest.getRepoPath())
            self.assertEquals(fileVolume.FileVolumeMetadata,
                              manifest.getVolumeClass())

    def test_getrepopath_with_iso_domain(self):
        with namedTemporaryDir() as tmpdir:
            metadata = FakeMetadata()
            metadata[sd.DMDK_CLASS] = sd.ISO_DOMAIN
            metadata[sd.DMDK_VERSION] = 3

            manifest = make_filesd_manifest(tmpdir, metadata)
            self.assertTrue(manifest.isISO())
            self.assertRaises(se.ImagesNotSupportedError, manifest.getRepoPath)

    def test_deleteimage(self):
        with namedTemporaryDir() as tmpdir:
            manifest = make_filesd_manifest(tmpdir)
            imguuid = str(uuid.uuid4())
            vols = get_uuid_list(3)
            imagepath = manifest.getImagePath(imguuid)
            for voluuid in vols:
                make_file_volume(manifest.domaindir, VOLSIZE, imguuid, voluuid)
                volpath = os.path.join(imagepath, voluuid)
                self.assertTrue(os.path.exists(volpath))
            manifest.deleteImage(manifest.sdUUID, imguuid, vols)
            self.assertFalse(os.path.exists(imagepath))

    def test_deleteimage_dir_not_empty(self):
        with namedTemporaryDir() as tmpdir:
            manifest = make_filesd_manifest(tmpdir)
            imguuid = str(uuid.uuid4())
            vols = get_uuid_list(2)
            imagepath = manifest.getImagePath(imguuid)
            for voluuid in vols:
                make_file_volume(manifest.domaindir, VOLSIZE, imguuid, voluuid)
                volpath = os.path.join(imagepath, voluuid)
                self.assertTrue(os.path.exists(volpath))
            self.assertRaises(se.ImageDeleteError, manifest.deleteImage,
                              manifest.sdUUID, imguuid, vols[1:])
            dirname, basename = os.path.split(imagepath)
            deldir = os.path.join(dirname, sd.REMOVED_IMAGE_PREFIX + basename)
            self.assertTrue(os.path.exists(deldir))

    def test_getallimages(self):
        template_volume, regular_volume = get_uuid_list(2)
        template_image, regular_image = get_uuid_list(2)
        vols = {template_volume: (regular_image, template_image),
                regular_volume: (regular_image,)}
        with namedTemporaryDir() as tmpdir:
            pooluuid = str(uuid.uuid4())
            pooldir = os.path.join(tmpdir, pooluuid)
            metadata = FakeMetadata()
            metadata[sd.DMDK_VERSION] = 3
            metadata[sd.DMDK_POOLS] = [pooluuid]
            manifest = make_filesd_manifest(pooldir, metadata)

            create_volume_tree(manifest, vols)
            with MonkeyPatchScope([(sd, 'storage_repository', tmpdir)]):
                self.assertEquals({regular_image, template_image},
                                  manifest.getAllImages())

    def test_getallimages_reject_file(self):
        with namedTemporaryDir() as tmpdir:
            pooluuid = str(uuid.uuid4())
            pooldir = os.path.join(tmpdir, pooluuid)
            metadata = FakeMetadata()
            metadata[sd.DMDK_VERSION] = 3
            metadata[sd.DMDK_POOLS] = [pooluuid]
            manifest = make_filesd_manifest(pooldir, metadata)

            imguuid = str(uuid.uuid4())
            imagepath = manifest.getImagePath(imguuid)
            with open(imagepath, 'w') as f:
                f.truncate(0)
        self.assertEquals(set(), manifest.getAllImages())

    def test_getallimages_reject_non_uuid(self):
        with namedTemporaryDir() as tmpdir:
            pooluuid = str(uuid.uuid4())
            pooldir = os.path.join(tmpdir, pooluuid)
            metadata = FakeMetadata()
            metadata[sd.DMDK_VERSION] = 3
            metadata[sd.DMDK_POOLS] = [pooluuid]
            manifest = make_filesd_manifest(pooldir, metadata)

            badimg = 'not-an-image'
            imagepath = manifest.getImagePath(badimg)
            os.makedirs(imagepath)
        self.assertEquals(set(), manifest.getAllImages())

    def test_getallimages_empty_dir(self):
        with namedTemporaryDir() as tmpdir:
            pooluuid = str(uuid.uuid4())
            pooldir = os.path.join(tmpdir, pooluuid)
            metadata = FakeMetadata()
            metadata[sd.DMDK_VERSION] = 3
            metadata[sd.DMDK_POOLS] = [pooluuid]
            manifest = make_filesd_manifest(pooldir, metadata)

            imguuid = str(uuid.uuid4())
            imagepath = manifest.getImagePath(imguuid)
            os.makedirs(imagepath)
        self.assertEquals(set(), manifest.getAllImages())

    def test_getallvolumes(self):
        template_volume, regular_volume = get_uuid_list(2)
        template_image, regular_image = get_uuid_list(2)
        vols = {template_volume: (regular_image, template_image),
                regular_volume: (regular_image,)}
        with namedTemporaryDir() as tmpdir:
            pooluuid = str(uuid.uuid4())
            pooldir = os.path.join(tmpdir, pooluuid)
            metadata = FakeMetadata()
            metadata[sd.DMDK_VERSION] = 3
            metadata[sd.DMDK_POOLS] = [pooluuid]
            manifest = make_filesd_manifest(pooldir, metadata)

            create_volume_tree(manifest, vols)
            with MonkeyPatchScope([(sd, 'storage_repository', tmpdir)]):
                allVols = manifest.getAllVolumes()

                # The template image must always come first
                images, parent = allVols[template_volume]
                self.assertEquals((template_image, regular_image), images)
                self.assertEquals(sd.BLANK_UUID, parent)

                images, parent = allVols[regular_volume]
                self.assertEquals((regular_image,), images)
                self.assertEquals(None, parent)


class BlockManifestTests(VdsmTestCase):
    MINIMUM_METADATA_SIZE = 512 * MB
    PV_UNUSABLE_SIZE = 384 * MB

    def test_getreaddelay(self):
        with namedTemporaryDir() as tmpdir:
            lvm = FakeLVM(tmpdir)
            with MonkeyPatchScope([(blockSD, 'lvm', lvm)]):
                manifest = make_blocksd_manifest()
                vg_name = manifest.sdUUID
                make_file(lvm.lvPath(vg_name, 'metadata'))
                self.assertIsInstance(manifest.getReadDelay(), float)

    def test_getvsize_active_lv(self):
        # Tests the path when the device file is present
        with namedTemporaryDir() as tmpdir:
            lvm = FakeLVM(tmpdir)
            with MonkeyPatchScope([(blockSD, 'lvm', lvm)]):
                manifest = make_blocksd_manifest(tmpdir)
                vg_name = make_vg(lvm, manifest)
                lv_name = str(uuid.uuid4())
                lvm.createLV(vg_name, lv_name, VOLSIZE)
                lvm.fake_lv_symlink_create(vg_name, lv_name)
                self.assertEqual(VOLSIZE,
                                 manifest.getVSize('<imgUUID>', lv_name))

    def test_getvsize_inactive_lv(self):
        # Tests the path when the device file is not present
        with namedTemporaryDir() as tmpdir:
            lvm = FakeLVM(tmpdir)
            with MonkeyPatchScope([(blockSD, 'lvm', lvm)]):
                manifest = make_blocksd_manifest(tmpdir)
                vg_name = make_vg(lvm, manifest)
                lv_name = str(uuid.uuid4())
                lvm.createLV(vg_name, lv_name, VOLSIZE)
                self.assertEqual(VOLSIZE,
                                 manifest.getVSize('<imgUUID>', lv_name))

    def test_getmetaparam(self):
        with namedTemporaryDir() as tmpdir:
            metadata = FakeMetadata()
            metadata[sd.DMDK_VERSION] = 3
            lvm = FakeLVM(tmpdir)
            with MonkeyPatchScope([(blockSD, 'lvm', lvm)]):
                manifest = make_blocksd_manifest(tmpdir, metadata)
                metadata[sd.DMDK_SDUUID] = manifest.sdUUID
                self.assertEquals(manifest.sdUUID,
                                  manifest.getMetaParam(sd.DMDK_SDUUID))

    def test_getblocksize_defaults(self):
        with namedTemporaryDir() as tmpdir:
            lvm = FakeLVM(tmpdir)
            with MonkeyPatchScope([(blockSD, 'lvm', lvm)]):
                manifest = make_blocksd_manifest(tmpdir)
                self.assertEquals(512, manifest.logBlkSize)
                self.assertEquals(512, manifest.phyBlkSize)

    def test_getblocksize(self):
        with namedTemporaryDir() as tmpdir:
            metadata = {sd.DMDK_VERSION: 3,
                        blockSD.DMDK_LOGBLKSIZE: 2048,
                        blockSD.DMDK_PHYBLKSIZE: 1024}
            lvm = FakeLVM(tmpdir)
            with MonkeyPatchScope([(blockSD, 'lvm', lvm)]):
                manifest = make_blocksd_manifest(tmpdir, metadata)
                self.assertEquals(2048, manifest.logBlkSize)
                self.assertEquals(1024, manifest.phyBlkSize)

    def test_metasize(self):
        """
        Test the check for enough free space to accommodate the metadata LV
        """
        with namedTemporaryDir() as tmpdir:
            lvm = FakeLVM(tmpdir)
            with MonkeyPatchScope([(blockSD, 'lvm', lvm)]):
                manifest = make_blocksd_manifest(tmpdir)
                vg_name = make_vg(lvm, manifest)
                # 512 is derived from calculations in the metaSize function.
                # Our small volume group will use the minimum allowed size.
                self.assertEquals(self.MINIMUM_METADATA_SIZE / MB,
                                  manifest.metaSize(vg_name))
                lvm.vgmd[vg_name]['free'] = self.MINIMUM_METADATA_SIZE - 1
                self.assertRaises(se.VolumeGroupSizeError,
                                  manifest.metaSize, vg_name)

    def test_getmetadatamapping_bad_metadata_extent(self):
        """
        We require the metadata LV to reside on the first extent of the first
        PV in the VG.  Check that we raise an error if this is not the case.
        """
        with namedTemporaryDir() as tmpdir:
            lvm = FakeLVM(tmpdir)
            with MonkeyPatchScope([(blockSD, 'lvm', lvm)]):
                manifest = make_blocksd_manifest(tmpdir)
                devices = get_random_devices(1)
                vg_name = make_vg(lvm, manifest, devices)

                # Fake a bad first extent (1) for our 'metadata' LV
                lvm.lvmd[vg_name][sd.METADATA]['devices'] = \
                    '/dev/mapper/{0}(1)'.format(devices[0])

                self.assertRaises(se.MetaDataMappingError,
                                  manifest.getMetaDataMapping, vg_name)

    def test_getmetadatamapping(self):
        """
        Validate the behavior of getMetadataMapping including:
         - pestart is 0 for all PVs in the VG (we override the real value 129M)
         - PVs' physical extents (pe) are mapped into the VG in the order that
           the PVs are added to the VG.  The mapoffset field shows the pe
           number in the VG map to which this PV's first pe maps.
        """
        with namedTemporaryDir() as tmpdir:
            lvm = FakeLVM(tmpdir)
            with MonkeyPatchScope([(blockSD, 'lvm', lvm)]):
                manifest = make_blocksd_manifest(tmpdir)
                devices = get_random_devices(3)
                vg_name = make_vg(lvm, manifest, devices)
                mapping = manifest.getMetaDataMapping(vg_name)
                # The values below assume that each device in the VG is 10G
                self.assertEquals(3, len(mapping))
                self.assertEquals(0, mapping['PV0']['pestart'])
                self.assertEquals(0, mapping['PV0']['mapoffset'])
                self.assertEquals(0, mapping['PV1']['pestart'])
                self.assertEquals(77, mapping['PV1']['mapoffset'])
                self.assertEquals(0, mapping['PV2']['pestart'])
                self.assertEquals(154, mapping['PV2']['mapoffset'])

    def test_getmetadatamapping_with_oldinfo(self):
        """
        When a dictionary of cached mapping info is supplied to
        getMetaDataMapping lvm will only be queried for devices which are not
        in the cached info.  Verify this behavior.
        """
        devices = get_random_devices(3)
        info = {'PV1': {'pestart': 0, 'mapoffset': 79, 'pecount': 79,
                        'guid': name_to_guid(devices[1]),
                        'uuid': '5CUMKd-yJDg-a1BY-Dhe1-llAo-Waxrr3'},
                'PV0': {'pestart': 0, 'mapoffset': 0, 'pecount': 79,
                        'guid': name_to_guid(devices[0]),
                        'uuid': '3ApdDe-dN8D-i6ay-cr60-uu3B-BxCclJ'}}

        with namedTemporaryDir() as tmpdir:
            lvm = FakeLVM(tmpdir)
            with MonkeyPatchScope([(blockSD, 'lvm', lvm)]):
                manifest = make_blocksd_manifest(tmpdir)
                vg_name = make_vg(lvm, manifest, devices)

                # Remove PV info from lvm to test that we rely on the info dict
                for i in 0, 1:
                    del lvm.pvmd[devices[i]]
                mapping = manifest.getMetaDataMapping(vg_name, info)

                self.assertEquals(3, len(mapping))
                self.assertEquals(name_to_guid(devices[2]),
                                  mapping['PV2']['guid'])
                for pv in ('PV0', 'PV1'):
                    self.assertEquals(info[pv], mapping[pv])

    def test_extendvolume(self):
        """
        Test the positive flow for extendVolume making sure the size is updated
        """
        with namedTemporaryDir() as tmpdir:
            lvm = FakeLVM(tmpdir)
            with MonkeyPatchScope([(blockSD, 'lvm', lvm)]):
                manifest = make_blocksd_manifest(tmpdir)
                vg_name = make_vg(lvm, manifest)
                lv = str(uuid.uuid4())
                lvm.createLV(vg_name, lv, VOLSIZE)
                self.assertEquals(VOLSIZE, manifest.getVSize('unused', lv))
                manifest.extendVolume(lv, 2 * VOLSIZE)
                self.assertEquals(2 * VOLSIZE, manifest.getVSize('unused', lv))

    def test_extend_maxpvs(self):
        """
        You may not extend a VG with additional devices if the number of PVs
        belonging to it would rise beyond an allowed limit.  Verify this check.
        """
        with namedTemporaryDir() as tmpdir:
            metadata = FakeMetadata()
            metadata[sd.DMDK_VERSION] = 0
            lvm = FakeLVM(tmpdir)
            with MonkeyPatchScope([(blockSD, 'lvm', lvm),
                                   (multipath, 'getMPDevNamesIter',
                                    lambda: iter(devices))]):
                manifest = make_blocksd_manifest(tmpdir, metadata)
                nr_initial_devices = 5
                devices = get_random_devices(nr_initial_devices)
                make_vg(lvm, manifest, devices)

                # Part of the test setup requires that the domain metadata
                # includes the PV mapping information for the original device
                # list.  Rather than hard-coding it, it's easier to use
                # updateMapping() which is designed for this purpose.
                manifest.updateMapping()
                new_dev_count = blockSD.MAX_PVS - nr_initial_devices + 1
                devices = get_random_devices(new_dev_count)
                self.assertRaises(se.StorageDomainIsMadeFromTooManyPVs,
                                  manifest.extend, devices, False)

    def test_extend_unknowndevs(self):
        """
        Devices which are not known to multipath cannot be used to extend a VG.
        """
        with namedTemporaryDir() as tmpdir:
            metadata = FakeMetadata()
            metadata[sd.DMDK_VERSION] = 3
            lvm = FakeLVM(tmpdir)
            devices = get_random_devices()
            with MonkeyPatchScope([(blockSD, 'lvm', lvm),
                                   (multipath, 'getMPDevNamesIter',
                                    lambda: list())]):
                manifest = make_blocksd_manifest(tmpdir, metadata)
                self.assertRaises(se.InaccessiblePhysDev,
                                  manifest.extend, devices, False)

    def test_extend(self):
        """
        Test the positive flow for extend and verify the new size and mapping
        """
        with namedTemporaryDir() as tmpdir:
            metadata = FakeMetadata()
            metadata[sd.DMDK_VERSION] = 3
            lvm = FakeLVM(tmpdir)
            with MonkeyPatchScope([(blockSD, 'lvm', lvm),
                                   (multipath, 'getMPDevNamesIter',
                                    lambda: iter(new_devices))]):
                manifest = make_blocksd_manifest(tmpdir, metadata)
                devices = get_random_devices(1)
                vg_name = make_vg(lvm, manifest, devices)
                new_devices = get_random_devices(1)
                manifest.updateMapping()
                manifest.extend(new_devices, False)
                newSize = 2 * (10 * GB - self.PV_UNUSABLE_SIZE)
                # TODO: Switch to using manifest.getStats when it is ready
                self.assertEquals(newSize, lvm.getVG(vg_name).size)
                self.assertEquals(2, len(manifest.getMetaDataMapping(vg_name)))

    def test_resizepv(self):
        """
        Test that resizePV handles device size update and metadata makes sense.
        """
        with namedTemporaryDir() as tmpdir:
            lvm = FakeLVM(tmpdir)
            with MonkeyPatchScope([(blockSD, 'lvm', lvm)]):
                manifest = make_blocksd_manifest(tmpdir)
                devices = get_random_devices(1)
                vg_name = make_vg(lvm, manifest, devices)
                # Set up the current mapping according to current device size
                manifest.updateMapping()
                pv = lvm.pvmd.values()[0]
                lvm.fake_pv_update_size(pv['name'], 20 * GB)
                manifest.resizePV(pv['guid'])
                self.assertEquals(self.MINIMUM_METADATA_SIZE / MB,
                                  manifest.metaSize(manifest.sdUUID))
                self.assertEquals(self.MINIMUM_METADATA_SIZE / MB,
                                  lvm.getLV(vg_name, sd.METADATA).size)
                newSize = 20 * GB - self.PV_UNUSABLE_SIZE
                self.assertEquals(newSize, lvm.getVG(vg_name).size)

    def test_metadata(self):
        with namedTemporaryDir() as tmpdir:
            metadata = FakeMetadata()
            metadata[sd.DMDK_ROLE] = sd.REGULAR_DOMAIN
            metadata[sd.DMDK_CLASS] = sd.DATA_DOMAIN
            metadata[sd.DMDK_TYPE] = sd.ISCSI_DOMAIN
            metadata[sd.DMDK_VERSION] = 3
            poolID = str(uuid.uuid4())
            metadata[sd.DMDK_POOLS] = [poolID]

            lvm = FakeLVM(tmpdir)
            with MonkeyPatchScope([(blockSD, 'lvm', lvm)]):
                manifest = make_blocksd_manifest(tmpdir, metadata)
                self.assertEquals(sd.REGULAR_DOMAIN, manifest.getDomainRole())
                self.assertEquals(sd.DATA_DOMAIN, manifest.getDomainClass())
                self.assertEquals(sd.ISCSI_DOMAIN, manifest.getStorageType())
                self.assertEquals(3, manifest.getVersion())
                self.assertEquals('3', manifest.getFormat())
                self.assertEquals(os.path.join(STORAGE_REPO, poolID),
                                  manifest.getRepoPath())
                self.assertEquals(blockVolume.BlockVolumeMetadata,
                                  manifest.getVolumeClass())

    def test_deleteimage(self):
        imguuid = str(uuid.uuid4())
        vols = get_uuid_list(3)
        vols_imgs = {vols[0]: sd.ImgsPar([imguuid], sd.BLANK_UUID),
                     vols[1]: sd.ImgsPar([imguuid], vols[0]),
                     vols[2]: sd.ImgsPar([imguuid], vols[1])}

        with namedTemporaryDir() as tmpdir:
            lvm = FakeLVM(tmpdir)
            with MonkeyPatchScope([(blockSD, 'lvm', lvm)]):
                manifest = make_blocksd_manifest(tmpdir)
                vg_name = make_vg(lvm, manifest)
                for lv_name, blocksdvol in vols_imgs.items():
                    lvm.createLV(vg_name, lv_name, VOLSIZE)
                    img_tag = blockVolume.TAG_PREFIX_IMAGE + imguuid
                    lvm.addtag(vg_name, lv_name, img_tag)

                imgpath = os.path.join(manifest.domaindir, sd.DOMAIN_IMAGES,
                                       imguuid)
                os.makedirs(imgpath)
                manifest.deleteImage(manifest.sdUUID, imguuid, vols_imgs)
                lvs = lvm.getLV(vg_name)
                self.assertEquals(1, len(lvs))
                self.assertEquals(sd.METADATA, lvs[0].name)

    @contextmanager
    def volume_layout(self, vols):
        with namedTemporaryDir() as tmpdir:
            lvm = FakeLVM(tmpdir)
            with MonkeyPatchScope([(blockSD, 'lvm', lvm)]):
                manifest = make_blocksd_manifest(tmpdir)
                vg_name = make_vg(lvm, manifest)
                create_lv_tree(lvm, vg_name, vols)
                yield manifest

    def test_getallimages(self):
        parent_vol, child_vol = get_uuid_list(2)
        template_image, regular_image = get_uuid_list(2)
        vols = {parent_vol: {'image': template_image, 'parent': sd.BLANK_UUID},
                child_vol: {'image': regular_image, 'parent': parent_vol}}
        with self.volume_layout(vols) as manifest:
            self.assertEquals({template_image, regular_image},
                              manifest.getAllImages())

    def test_getallvolumes(self):
        template_volume, first_volume, second_volume = get_uuid_list(3)
        template_image, regular_image = get_uuid_list(2)
        vols = {template_volume: {
                'image': template_image,
                'parent': sd.BLANK_UUID},
                first_volume: {
                'image': regular_image,
                'parent': template_volume},
                second_volume: {
                'image': regular_image,
                'parent': first_volume}}
        with self.volume_layout(vols) as manifest:
            allvols = manifest.getAllVolumes()
            self.assertEquals(set(vols.keys()), set(allvols.keys()))

            # The template image must always come first
            images = [template_image, regular_image]
            self.assertEquals(images, allvols[template_volume].imgs)
            self.assertEquals(sd.BLANK_UUID,
                              allvols[template_volume].parent)

            self.assertEquals([regular_image], allvols[first_volume].imgs)
            self.assertEquals(template_volume,
                              allvols[first_volume].parent)
            self.assertEquals([regular_image], allvols[second_volume].imgs)
            self.assertEquals(first_volume,
                              allvols[second_volume].parent)

    def test_getallvolumes_removed_template_image(self):
        vol_id, img_id = get_uuid_list(2)
        removed_img_id = sd.REMOVED_IMAGE_PREFIX + img_id
        vols = {vol_id: {'image': removed_img_id, 'parent': sd.BLANK_UUID}}
        with self.volume_layout(vols) as manifest:
            vols, remnants = manifest.getAllVolumesImages()
            self.assertEquals(0, len(vols))
            self.assertIn(vol_id, remnants)

    def test_getallvolumes_removed_volume(self):
        vol_id, img_id = get_uuid_list(2)
        removed_vol_id = sd.REMOVED_IMAGE_PREFIX + vol_id
        vols = {removed_vol_id: {'image': img_id, 'parent': sd.BLANK_UUID}}
        with self.volume_layout(vols) as manifest:
            vols, remnants = manifest.getAllVolumesImages()
            self.assertEquals(0, len(vols))
            self.assertIn(removed_vol_id, remnants)

    def test_getallvolumes_image_with_removed_volumes(self):
        vol_a_id, vol_b_id, img_id = get_uuid_list(3)
        removed_vol_id = sd.REMOVED_IMAGE_PREFIX + vol_b_id
        vols = {vol_a_id: {'image': img_id, 'parent': sd.BLANK_UUID},
                removed_vol_id: {'image': img_id, 'parent': vol_a_id}}
        with self.volume_layout(vols) as manifest:
            vols, remnants = manifest.getAllVolumesImages()
            self.assertIn(vol_a_id, vols)
            self.assertIn(removed_vol_id, remnants)

    def test_getallvolumes_ignore_missing_tags(self):
        vol_a_id, vol_b_id, img_id = get_uuid_list(3)
        vols = {vol_a_id: {'image': img_id},
                vol_b_id: {'parent': sd.BLANK_UUID}}
        with self.volume_layout(vols) as manifest:
                allvols = manifest.getAllVolumes()
                self.assertNotIn(vol_a_id, allvols)
                self.assertNotIn(vol_b_id, allvols)

    def test_getallvolumes_broken_image(self):
        vol_id, img_id, bad_vol_id = get_uuid_list(3)
        vols = {vol_id: {'image': img_id, 'parent': bad_vol_id}}
        with self.volume_layout(vols) as manifest:
                # This doesn't raise an error, just prints a warning to logs
                allvols = manifest.getAllVolumes()
                self.assertNotIn(bad_vol_id, allvols)
                self.assertEquals(bad_vol_id, allvols[vol_id].parent)


class BlockDomainMetadataSlotTests(VdsmTestCase):

    def test_metaslot_selection(self):
        with namedTemporaryDir() as tmpdir:
            lvm = FakeLVM(tmpdir)
            with MonkeyPatchScope([(blockSD, 'lvm', lvm)]):
                manifest = make_blocksd_manifest(tmpdir)
                make_vg(lvm, manifest)
                lvs = get_uuid_list(2)
                for lv, offset in zip(lvs, [4, 7]):
                    lvm.createLV(manifest.sdUUID, lv, VOLSIZE)
                    tag = blockVolume.TAG_PREFIX_MD + str(offset)
                    lvm.addtag(manifest.sdUUID, lv, tag)
                with manifest.acquireVolumeMetadataSlot(None, 1) as mdSlot:
                    self.assertEqual(mdSlot, 5)

    def test_metaslot_lock(self):
        with namedTemporaryDir() as tmpdir:
            lvm = FakeLVM(tmpdir)
            with MonkeyPatchScope([(blockSD, 'lvm', lvm)]):
                manifest = make_blocksd_manifest(tmpdir)
                make_vg(lvm, manifest)
                with manifest.acquireVolumeMetadataSlot(None, 1):
                    acquired = manifest._lvTagMetaSlotLock.acquire(False)
                    self.assertEqual(acquired, False)


def name_to_guid(name):
    return os.path.basename(name)
