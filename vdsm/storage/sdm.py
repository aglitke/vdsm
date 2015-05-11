#
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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
#
# Refer to the README and COPYING files for full details of the license
#

import logging
import os

from clientIF import clientIF as cif
from image import ImageManifest
import resourceManager as rm
from resourceFactories import IMAGE_NAMESPACE
import sd
import storage_exception as se
import volume

from vdsm import qemuimg

log = logging.getLogger('sdm')

rmanager = rm.ResourceManager.getInstance()


def get_domain_host_id(sdUUID):
    # FIXME: irs._obj is an ugly hack, domainMonitor needs internal locking
    return cif.getInstance().irs._obj.domainMonitor.getHostId(sdUUID)


def create_volume_container(dom_manifest, img_id, size, vol_format, disk_type,
                            vol_id, desc, src_img_id, src_vol_id):
    hostId = get_domain_host_id(dom_manifest.sdUUID)
    dom_manifest.acquireDomainLock(hostId)
    imageResourcesNamespace = sd.getNamespace(dom_manifest.sdUUID,
                                              IMAGE_NAMESPACE)
    try:
        with rmanager.acquireResource(imageResourcesNamespace, img_id,
                                      rm.LockType.exclusive):
            image_manifest = ImageManifest(dom_manifest.getRepoPath())
            img_path = image_manifest.create_image_dir(dom_manifest.sdUUID,
                                                       img_id)
            vol_parent_md = None
            if src_vol_id != volume.BLANK_UUID:
                # When the src_img_id isn't specified we assume it's the same
                # as img_id
                if src_img_id == volume.BLANK_UUID:
                    src_img_id = img_id
                vol_parent_md = dom_manifest.produceVolume(src_img_id,
                                                           src_vol_id)
                size = vol_parent_md.getSize()

            dom_manifest.create_volume_artifacts(img_id, vol_id, size,
                                                 vol_format,  disk_type,
                                                 desc, src_vol_id)
            if src_vol_id != volume.BLANK_UUID:
                _prepare_volume_for_parenthood(vol_parent_md, src_img_id,
                                               src_vol_id, img_id, img_path)

            # TODO: get actual size from create_volume_artifacts retval
            size = volume.VolumeMetadata.adjust_new_volume_size(
                dom_manifest, img_id, vol_id, size, vol_format)

            vol_path = os.path.join(img_path, vol_id)
            _initialize_volume_contents(img_id, vol_id, vol_path, vol_format,
                                        size, vol_parent_md)
            dom_manifest.commit_volume_artifacts(img_path, img_id, vol_id)
    finally:
        dom_manifest.releaseDomainLock()


def _parent_is_template(parent_img_id, child_img_id):
    # When creating a new volume, using BLANK_UUID for the parent image is
    # shorthand for indicating that the parent belongs to the same image.
    return parent_img_id != child_img_id and parent_img_id != volume.BLANK_UUID


def _make_template_shareable(vol_md):
    imageResourcesNamespace = sd.getNamespace(vol_md.sdUUID,
                                              IMAGE_NAMESPACE)
    if not vol_md.isShared():
        if vol_md.getParentId() == volume.BLANK_UUID:
            with rmanager.acquireResource(imageResourcesNamespace,
                                          vol_md.imgUUID,
                                          rm.LockType.exclusive):
                vol_md.setShared()
        else:
            raise se.VolumeNonShareable(vol_md.volUUID)


def _share_template_to_image(tvol_md, img_path):
    if not tvol_md.isShared():
        raise se.VolumeNonShareable(tvol_md.volUUID)

    log.debug("Share volume %s to %s", tvol_md.volUUID, img_path)
    if os.path.basename(tvol_md.imagePath) == os.path.basename(img_path):
        raise se.VolumeOwnershipError(tvol_md.volUUID)

    try:
        tvol_md.share_to_image(img_path)
    except Exception as e:
        vol_path = os.path.join(img_path, tvol_md.volUUID)
        raise se.CannotShareVolume(tvol_md.getVolumePath(), vol_path, str(e))


def _prepare_volume_for_parenthood(vol_md, img_id, vol_id, child_img_id,
                                   child_img_path):
    if not vol_md.isLegal():
        raise se.createIllegalVolumeSnapshotError(vol_id)

    if _parent_is_template(img_id, child_img_id):
        _make_template_shareable(vol_md)
        _share_template_to_image(vol_md, child_img_path)
    return vol_md


def _clone_volume(src_vol_md, dst_vol_path, dst_vol_format):
    wasleaf = False
    if src_vol_md.isLeaf():
        wasleaf = True
        src_vol_md.setInternal()
    src_vol_md.prepare(rw=False)
    try:
        log.debug('cloning volume %s to %s', src_vol_md.volumePath,
                  dst_vol_path)
        parent = volume.getBackingVolumePath(src_vol_md.imgUUID,
                                             src_vol_md.volUUID)
        qemuimg.create(dst_vol_path, backing=parent,
                       format=volume.fmt2str(dst_vol_format),
                       backingFormat=volume.fmt2str(src_vol_md.getFormat()))
    except Exception as e:
        log.exception('cannot clone image %s volume %s to %s',
                      src_vol_md.imgUUID, src_vol_md.volUUID, dst_vol_path)
        # FIXME: might race with other clones
        if wasleaf:
            src_vol_md.setLeaf()
        raise se.CannotCloneVolume(src_vol_md.volumePath, dst_vol_path, str(e))
    finally:
        src_vol_md.teardown(src_vol_md.sdUUID, src_vol_md.volUUID)


def _initialize_volume_contents(img_id, vol_id, vol_path, vol_format, size,
                                parent_vol_md):
    if not parent_vol_md:
        log.info("Request to create %s volume %s with size = %s "
                 "sectors", volume.type2name(vol_format),
                 vol_path, size)
        if vol_format == volume.COW_FORMAT:
            qemuimg.create(vol_path, size * volume.BLOCK_SIZE,
                           volume.fmt2str(vol_format))
    else:
        log.info("Request to create snapshot %s/%s of volume "
                 "%s/%s", img_id, vol_id, parent_vol_md.imgUUID,
                 parent_vol_md.volUUID)
        _clone_volume(parent_vol_md, vol_path, vol_format)
