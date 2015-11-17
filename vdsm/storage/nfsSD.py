#
# Copyright 2009-2011 Red Hat, Inc.
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

import sd
import fileSD
import fileUtils
import storage_exception as se
import outOfProcess as oop
import mount
import misc


class NfsStorageDomain(fileSD.FileStorageDomain):

    @classmethod
    def _preCreateValidation(cls, sdUUID, domPath, typeSpecificArg,
                             storageType, version):
        # Some trivial resource validation
        # TODO Checking storageType==nfs in the nfs class is not clean
        if storageType == sd.NFS_DOMAIN and ":" not in typeSpecificArg:
            raise se.StorageDomainIllegalRemotePath(typeSpecificArg)

        sd.validateDomainVersion(version)

        # Make sure the underlying file system is mounted
        if not mount.isMounted(domPath):
            raise se.StorageDomainFSNotMounted(domPath)

        fileSD.validateDirAccess(domPath)
        fileSD.validateFileSystemFeatures(sdUUID, domPath)

        # Make sure there are no remnants of other domain
        mdpat = os.path.join(domPath, "*", sd.DOMAIN_META_DATA)
        if len(oop.getProcessPool(sdUUID).glob.glob(mdpat)) > 0:
            raise se.StorageDomainNotEmpty(typeSpecificArg)

    @classmethod
    def create(cls, sdUUID, domainName, domClass, remotePath, storageType,
               version):
        """
        Create new storage domain.
            'sdUUID' - Storage Domain UUID
            'domainName' - storage domain name ("iso" or "data domain name")
            'domClass' - Data/Iso
            'remotePath' - server:/export_path
            'storageType' - NFS_DOMAIN, LOCALFS_DOMAIN, &etc.
            'version' - DOMAIN_VERSIONS
        """
        cls.log.info("sdUUID=%s domainName=%s remotePath=%s "
                     "domClass=%s", sdUUID, domainName, remotePath, domClass)

        if not misc.isAscii(domainName) and not sd.supportsUnicode(version):
            raise se.UnicodeArgumentException()

        # Create local path
        mntPath = fileUtils.transformPath(remotePath)

        mntPoint = cls.getMountPoint(mntPath)

        cls._preCreateValidation(sdUUID, mntPoint, remotePath, storageType,
                                 version)

        domainDir = os.path.join(mntPoint, sdUUID)
        cls._prepareMetadata(domainDir, sdUUID, domainName, domClass,
                             remotePath, storageType, version)

        # create domain images folder
        imagesDir = os.path.join(domainDir, sd.DOMAIN_IMAGES)
        oop.getProcessPool(sdUUID).fileUtils.createdir(imagesDir)

        # create special imageUUID for ISO/Floppy volumes
        if domClass is sd.ISO_DOMAIN:
            isoDir = os.path.join(imagesDir, sd.ISO_IMAGE_UUID)
            oop.getProcessPool(sdUUID).fileUtils.createdir(isoDir)

        fsd = cls(os.path.join(mntPoint, sdUUID))
        fsd.initSPMlease()

        return fsd

    @classmethod
    def getMountPoint(cls, mountPath):
        return os.path.join(cls.storage_repository, sd.DOMAIN_MNT_POINT,
                            mountPath)

    @staticmethod
    def findDomainPath(sdUUID):
        for tmpSdUUID, domainPath in fileSD.scanDomains("*"):
            if tmpSdUUID == sdUUID and mount.isMounted(
                    os.path.join(domainPath, "..")):
                return domainPath

        raise se.StorageDomainDoesNotExist(sdUUID)

    def getRealPath(self):
        try:
            return mount.getMountFromTarget(self.mountpoint).fs_spec
        except mount.MountError:
            return ""


def findDomain(sdUUID):
    return NfsStorageDomain(NfsStorageDomain.findDomainPath(sdUUID))


def findDomainManifest(sdUUID, metadata=None):
    return fileSD.FileStorageDomainManifest(
        NfsStorageDomain.findDomainPath(sdUUID), metadata)
