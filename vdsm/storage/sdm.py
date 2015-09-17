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
from contextlib import contextmanager
import threading
import time

from clientIF import clientIF as cif
import jobs
from image import ImageManifest
import resourceManager as rm
from resourceFactories import IMAGE_NAMESPACE
import sd
import storage_exception as se
import image
import volume

from vdsm import qemuimg

from misc import maybe_fail

log = logging.getLogger('sdm')

rmanager = rm.ResourceManager.getInstance()


def get_domain_host_id(sdUUID):
    # FIXME: irs._obj is an ugly hack, domainMonitor needs internal locking
    return cif.getInstance().irs._obj.domainMonitor.getHostId(sdUUID)


def create_volume_container(dom_manifest, img_id, size, vol_format, disk_type,
                            vol_id, desc, src_img_id, src_vol_id):
    maybe_fail(0.005)
    hostId = get_domain_host_id(dom_manifest.sdUUID)
    maybe_fail(0.02)
    dom_manifest.acquireDomainLock(hostId)
    imageResourcesNamespace = sd.getNamespace(dom_manifest.sdUUID,
                                              IMAGE_NAMESPACE)
    try:
        maybe_fail(0.02)
        with rmanager.acquireResource(imageResourcesNamespace, img_id,
                                      rm.LockType.exclusive):
            maybe_fail(0.02)
            image_manifest = ImageManifest(dom_manifest.getRepoPath())
            maybe_fail(0.02)
            img_path = image_manifest.create_image_dir(dom_manifest.sdUUID,
                                                       img_id)
            maybe_fail(0.02)
            vol_parent_md = None
            maybe_fail(0.02)
            if src_vol_id != volume.BLANK_UUID:
                maybe_fail(0.02)
                # When the src_img_id isn't specified we assume it's the same
                # as img_id
                if src_img_id == volume.BLANK_UUID:
                    maybe_fail(0.02)
                    src_img_id = img_id
                maybe_fail(0.02)
                vol_parent_md = dom_manifest.produceVolume(src_img_id,
                                                           src_vol_id)
                maybe_fail(0.02)
                size = vol_parent_md.getSize()
                maybe_fail(0.02)

            dom_manifest.create_volume_artifacts(img_id, vol_id, size,
                                                 vol_format,  disk_type,
                                                 desc, src_vol_id)
            maybe_fail(0.02)
            if src_vol_id != volume.BLANK_UUID:
                maybe_fail(0.02)
                _prepare_volume_for_parenthood(vol_parent_md, src_img_id,
                                               src_vol_id, img_id, img_path)
            maybe_fail(0.02)

            # TODO: get actual size from create_volume_artifacts retval
            size = volume.VolumeMetadata.adjust_new_volume_size(
                dom_manifest, img_id, vol_id, size, vol_format)
            maybe_fail(0.02)

            vol_path = os.path.join(img_path, vol_id)
            maybe_fail(0.02)
            _initialize_volume_contents(img_id, vol_id, vol_path, vol_format,
                                        size, vol_parent_md)
            maybe_fail(0.02)
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
        log.debug("Converting leaf volume %s to template", vol_md.volUUID)
        if vol_md.getParentId() == volume.BLANK_UUID:
            with rmanager.acquireResource(imageResourcesNamespace,
                                          vol_md.imgUUID,
                                          rm.LockType.exclusive):
                # XXX: How will we undo this operation safely?
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


def remove_volume(dom_manifest, img_id, vol_id):
    hostId = get_domain_host_id(dom_manifest.sdUUID)
    dom_manifest.acquireDomainLock(hostId)
    try:
        res_ns = sd.getNamespace(dom_manifest.sdUUID, IMAGE_NAMESPACE)
        with rmanager.acquireResource(res_ns, img_id, rm.LockType.exclusive):
            vol = dom_manifest.produceVolume(img_id, vol_id)
            vol.validate()
            vol.validateDelete()
            parent_id = vol.getParentId()
            dom_manifest.discard_volume(img_id, vol_id, parent_id)

            # Removing the last volume may have caused this image to become
            # garbage.  As an optimization, clean up the image now.  In case
            # this code does not run, the image will be cleaned by the periodic
            # garbage collector.
            garbage_imgs = dom_manifest.get_garbage_images(img_id)
            if len(garbage_imgs) == 1:
                _garbage_collect_image(dom_manifest, garbage_imgs[0])
    finally:
        dom_manifest.releaseDomainLock()


def remove_image(dom_manifest, img_id):
    hostId = get_domain_host_id(dom_manifest.sdUUID)
    dom_manifest.acquireDomainLock(hostId)
    try:
        allvols = dom_manifest.getAllVolumes()
        volsbyimg = sd.getVolsOfImage(allvols, img_id)
        if not volsbyimg:
            log.warning("Empty or not found image %s in SD %s.",
                        img_id, dom_manifest.sdUUID)
            raise se.ImageDoesNotExistInSD(img_id, dom_manifest.sdUUID)

        # Images should not be deleted if they are templates being used by
        # other images.
        for k, v in volsbyimg.iteritems():
            if len(v.imgs) > 1 and v.imgs[0] == img_id:
                raise se.CannotDeleteSharedVolume("Cannot delete shared "
                                                  "image %s. volsbyimg: %s" %
                                                  (img_id, volsbyimg))

        res_ns = sd.getNamespace(dom_manifest.sdUUID, IMAGE_NAMESPACE)
        with rmanager.acquireResource(res_ns, img_id, rm.LockType.exclusive):
            dom_manifest.deleteImage(dom_manifest.sdUUID, img_id, volsbyimg)
    finally:
        dom_manifest.releaseDomainLock()


def garbage_collect_storage_domain(dom_manifest):
    # This garbage collector runs periodically on the storage domain to
    # clean up interrupted volume removal operations and remove images
    # which are no longer needed.

    if dom_manifest.isISO():
        log.debug("Ignoring request to garbage collect ISO domain %s",
                  dom_manifest.sdUUID)
        return

    hostId = get_domain_host_id(dom_manifest.sdUUID)
    dom_manifest.acquireDomainLock(hostId)
    try:
        # Remove any remnant image directories
        dom_manifest.imageGarbageCollector()
        garbage_collect_volumes(dom_manifest)
        garbage_collect_images(dom_manifest)
    finally:
        dom_manifest.releaseDomainLock()


def _garbage_collect_image(dom_manifest, gc_img):
    # If we raced with create_volume_container this image may not
    # actually be garbage.  If getImageVolumes returns an empty
    # list, then the image either contains an orphaned template or
    # it is completely empty.  If the list contains any volumes
    # then the image should not be garbage collected.
    sd_id = dom_manifest.sdUUID
    cur_vols = dom_manifest.getVolumeClass().getImageVolumes(
        dom_manifest.getRepoPath(), sd_id, gc_img.image)
    if len(cur_vols) > 0:
        log.debug("Aborting garbage collection of image %s which "
                  "now contains volumes %s", gc_img.image, cur_vols)
        return
    log.info("Garbage collecting image %s", gc_img)
    dom_manifest.deleteImage(sd_id, gc_img.image, gc_img.vols)


def garbage_collect_images(dom_manifest):
    res_ns = sd.getNamespace(dom_manifest.sdUUID, IMAGE_NAMESPACE)
    for gc_img in dom_manifest.get_garbage_images():
        with rmanager.acquireResource(res_ns, gc_img.image,
                                      rm.LockType.exclusive):
            _garbage_collect_image(dom_manifest, gc_img)


def garbage_collect_volumes(dom_manifest, img_filter=None, vol_filter=None):
    # This garbage collector cleans up one or more volumes on a storage
    # domain which have been partially removed.  In the typical case it is
    # invoked with a specific volume UUID immediately after that volume has
    # been removed.  Care should be taken to ensure that this common case
    # remains fast and efficient.  This can also be called by the periodic
    # garbage collector to clean up interrupted operations.
    # TODO: Acquire resource locks (imagine this being called concurrently)
    res_ns = sd.getNamespace(dom_manifest.sdUUID, IMAGE_NAMESPACE)
    for gcvol in dom_manifest.get_garbage_volumes(img_filter, vol_filter):
        with rmanager.acquireResource(res_ns, gcvol.image,
                                      rm.LockType.exclusive):
            # get_garbage_volumes runs without locking so it's possible that
            # the volume we are about to garbage collect is no longer garbage.
            # If we can produce the volume then do not garbage collect it.
            try:
                dom_manifest.produceVolume(gcvol.image, gcvol.name)
            except se.VolumeDoesNotExist:
                pass
            else:
                log.debug("Skipping volume: %s image: %s which is not garbage",
                          gcvol.name, gcvol.image)
                continue

            dom_manifest.garbage_collect_volume(gcvol.image, gcvol.name,
                                                gcvol.meta_id, gcvol.parent)


class CopyDataStatus(jobs.STATUS):
    '''
    COPYING: Copying data
    '''
    COPYING = 'copying'


class CopyDataJob(jobs.Job):
    _JOB_TYPE = 'storage'
    _LOG_INTERVAL = 60

    def __init__(self, job_id, src_vol, src_res, dst_vol, dst_res, collapse):
        super(CopyDataJob, self).__init__(job_id)
        self._src_vol = src_vol
        self._dst_vol = dst_vol
        self._src_res = src_res
        self._dst_res = dst_res
        self._collapse = collapse
        self._progress = 0

    def progress(self):
        return self._progress

    def run(self):
        self._prepare()
        self._copy()
        self._cleanup()

    def _prepare(self):
        pass

    def _copy(self):
        pass

    def _cleanup(self):
        if self.status == jobs.STATUS.DONE:
            self._dst_vol.setLegality(volume.LEGAL_VOL)
        self._dst_vol.teardown(self._dst_vol.sdUUID, self._dst_vol.volUUID)
        self._src_vol.teardown(self._src_vol.sdUUID, self._src_vol.volUUID)
        self._dst_vol.releaseVolumeLease()
        self._dst_res.release()
        self._src_vol.releaseVolumeLease()
        self._src_res.release()


@contextmanager
def _copy_data_secured_volume(dom_manifest, img_id, vol_id, lock_type):
    """
    Secure a volume for use by copy_data.  The image resource must be locked
    and the volume lease taken.  These will be held for the CopyDataJob and
    released only if there is an error while preparing.  Upon completion,
    CopyDataJob will release these.
    """
    res_ns = sd.getNamespace(dom_manifest.sdUUID, IMAGE_NAMESPACE)
    res = rmanager.acquireResource(res_ns, img_id, lock_type)
    try:
        dom_manifest.acquireVolumeLease(img_id, vol_id)
        try:
            yield dom_manifest.produceVolume(img_id, vol_id)
        except:
            log.exception("Releasing volume lease sd: %s img: %s vol: %s",
                          dom_manifest.sdUUID, img_id, vol_id)
            dom_manifest.releaseVolumeLease(img_id, vol_id)
            raise
    except:
        log.exception("Releasing resource %s", res)
        res.release()
        raise


def _copy_data_calc_extend_size():
    return None


def copy_data(src_manifest, src_img_id, src_vol_id,
              dst_manifest, dst_img_id, dst_vol_id, collapse):
    try:
        with _copy_data_secured_volume(src_manifest, src_img_id, src_vol_id,
                                       rm.LockType.shared) as src_vol:
            with _copy_data_secured_volume(dst_manifest, dst_img_id,
                                           dst_vol_id,
                                           rm.LockType.exclusive) as dst_vol:
                req_size = _copy_data_calc_extend_size()
                if req_size:
                    log.info("Extending target volume %s to size %i",
                             dst_vol_id, req_size)
                    host_id = get_domain_host_id(dst_manifest.sdUUID)
                    dst_manifest.acquireDomainLock(host_id)
                    try:
                        # Requested size must be converted from bytes to MB
                        dst_manifest.extendVolume(dst_vol_id, req_size >> 20)
                    finally:
                        dst_manifest.releaseDomainLock()

                src_vol.prepare(rw=False, justme=only_vol)
                try:
                    dst_vol.prepare(rw=True, justme=True)
                    try:
                        dst_vol.setLegality(volume.ILLEGAL_VOL)

                        # Start CopDataJob
                    except:
                        dst_vol.teardown(dst_manifest.sdUUID, dst_vol_id)
                        raise
                except:
                    src_vol.teardown(src_manifest.sdUUID, src_vol_id)
                    raise
    except Exception:
        log.exception("Copy image error")
        raise se.CopyImageError()
