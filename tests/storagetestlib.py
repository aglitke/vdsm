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

from testlib import make_file, namedTemporaryDir
from storagefakelib import FakeLVM
from monkeypatch import MonkeyPatchScope

from vdsm import utils

from storage import sd, blockSD, fileSD, image, volume, blockVolume
from storage.sdm import volume_artifacts


NR_PVS = 2       # The number of fake PVs we use to make a fake VG by default
MDSIZE = 524288  # The size (in bytes) of fake metadata files
MB = 1024 ** 2   # Used to convert bytes to MB


class FakeEnv(object):
    def __init__(self, sd_manifest, lvm=None):
        self.sd_manifest = sd_manifest
        if lvm:
            self.lvm = lvm


@contextmanager
def fake_file_env(obj=None):
    with namedTemporaryDir() as tmpdir:
        sd_manifest = make_filesd_manifest(tmpdir)
        with MonkeyPatchScope([
            [sd, 'storage_repository', tmpdir]
        ]):

            yield FakeEnv(sd_manifest)


@contextmanager
def fake_block_env(obj=None):
    with namedTemporaryDir() as tmpdir:
        lvm = FakeLVM(tmpdir)
        with MonkeyPatchScope([
            (blockSD, 'lvm', lvm),
            (blockVolume, 'lvm', lvm),
            (volume_artifacts, 'lvm', lvm),
            (sd, 'storage_repository', tmpdir),
        ]):
            sd_manifest = make_blocksd_manifest(tmpdir, lvm)
            yield FakeEnv(sd_manifest, lvm=lvm)


class FakeMetadata(dict):
    @contextmanager
    def transaction(self):
        yield


def make_sd_metadata(sduuid, version=3, dom_class=sd.DATA_DOMAIN, pools=None):
    md = FakeMetadata()
    md[sd.DMDK_SDUUID] = sduuid
    md[sd.DMDK_VERSION] = version
    md[sd.DMDK_CLASS] = dom_class
    md[sd.DMDK_POOLS] = pools if pools is not None else [str(uuid.uuid4())]
    return md


def make_blocksd_manifest(tmpdir, fake_lvm, sduuid=None, devices=None):
    if sduuid is None:
        sduuid = str(uuid.uuid4())
    if devices is None:
        devices = get_random_devices()

    fake_lvm.createVG(sduuid, devices, blockSD.STORAGE_DOMAIN_TAG,
                      blockSD.VG_METADATASIZE)
    fake_lvm.createLV(sduuid, sd.METADATA, blockSD.SD_METADATA_SIZE)

    # Create the rest of the special LVs
    for metafile, sizemb in sd.SPECIAL_VOLUME_SIZES_MIB.iteritems():
        fake_lvm.createLV(sduuid, metafile, sizemb)
    fake_lvm.createLV(sduuid, blockSD.MASTERLV, blockSD.MASTERLV_SIZE)

    # We'll store the domain metadata in the VG's tags
    metadata = make_sd_metadata(sduuid)
    assert(metadata[sd.DMDK_VERSION] >= 3)  # Tag based MD is V3 and above
    tag_md = blockSD.TagBasedSDMetadata(sduuid)
    tag_md.update(metadata)

    manifest = blockSD.BlockStorageDomainManifest(sduuid, tag_md)
    manifest.mountpoint = os.path.join(tmpdir, sd.DOMAIN_MNT_POINT,
                                       sd.BLOCKSD_DIR)
    manifest.domaindir = tmpdir
    os.makedirs(os.path.join(manifest.domaindir, sduuid, sd.DOMAIN_IMAGES))

    # Make the mountpoint directory structure
    os.makedirs(os.path.join(manifest.mountpoint, sduuid, sd.DOMAIN_IMAGES))
    return manifest


def get_random_devices(count=NR_PVS):
    return ['/dev/mapper/{0}'.format(os.urandom(16).encode('hex'))
            for _ in range(count)]


def get_metafile_path(domaindir):
    return os.path.join(domaindir, sd.DOMAIN_META_DATA, sd.METADATA)


def make_filesd_manifest(tmpdir):
    spuuid = str(uuid.uuid4())
    sduuid = str(uuid.uuid4())

    domain_path = os.path.join(tmpdir, spuuid, sduuid)
    metafile = get_metafile_path(domain_path)
    make_file(metafile)
    metadata = fileSD.FileSDMetadata(metafile)
    metadata.update(make_sd_metadata(sduuid, pools=[spuuid]))

    manifest = fileSD.FileStorageDomainManifest(domain_path, metadata)
    os.makedirs(os.path.join(manifest.domaindir, sd.DOMAIN_IMAGES))
    return manifest


def make_file_volume(sd_manifest, size, imguuid, voluuid,
                     parent_vol_id=volume.BLANK_UUID,
                     vol_format=volume.RAW_FORMAT,
                     prealloc=volume.SPARSE_VOL,
                     disk_type=image.UNKNOWN_DISK_TYPE,
                     desc='fake volume'):
    volpath = os.path.join(sd_manifest.domaindir, "images", imguuid, voluuid)
    mdfiles = [volpath + '.meta', volpath + '.lease']
    make_file(volpath, size)
    for mdfile in mdfiles:
        make_file(mdfile)

    size_blk = size / volume.BLOCK_SIZE
    vol_class = sd_manifest.getVolumeClass()
    vol_class.newMetadata(
        (volpath,),
        sd_manifest.sdUUID,
        imguuid,
        parent_vol_id,
        size_blk,
        volume.type2name(vol_format),
        volume.type2name(prealloc),
        volume.type2name(volume.LEAF_VOL),
        disk_type,
        desc,
        volume.LEGAL_VOL)


def make_block_volume(lvm, sd_manifest, size, imguuid, voluuid,
                      parent_vol_id=volume.BLANK_UUID,
                      vol_format=volume.RAW_FORMAT,
                      prealloc=volume.PREALLOCATED_VOL,
                      disk_type=image.UNKNOWN_DISK_TYPE,
                      desc='fake volume'):
    sduuid = sd_manifest.sdUUID
    image_manifest = image.ImageManifest(sd_manifest.getRepoPath())
    imagedir = image_manifest.getImageDir(sduuid, imguuid)
    os.makedirs(imagedir)
    size_mb = utils.round(size, MB)
    size_blk = size_mb * MB / volume.BLOCK_SIZE
    lvm.createLV(sduuid, voluuid, size_mb)
    with sd_manifest.acquireVolumeMetadataSlot(
            voluuid, blockVolume.VOLUME_MDNUMBLKS) as slot:
        lvm.addtag(sduuid, voluuid, "%s%s" % (blockVolume.TAG_PREFIX_MD, slot))
        lvm.addtag(sduuid, voluuid, "%s%s" % (blockVolume.TAG_PREFIX_PARENT,
                                              volume.BLANK_UUID))
        lvm.addtag(sduuid, voluuid, "%s%s" % (blockVolume.TAG_PREFIX_IMAGE,
                                              imguuid))

    vol_class = sd_manifest.getVolumeClass()
    vol_class.newMetadata(
        (sduuid, slot),
        sduuid,
        imguuid,
        parent_vol_id,
        size_blk,
        volume.type2name(vol_format),
        volume.type2name(prealloc),
        volume.type2name(volume.LEAF_VOL),
        disk_type,
        desc,
        volume.LEGAL_VOL)
