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

from contextlib import contextmanager

from testlib import VdsmTestCase
from testlib import permutations, expandPermutations
from testlib import recorded

from storage import sd, blockSD, fileSD, image, volume, fileVolume, blockVolume


class FakeDomainManifest(sd.StorageDomainManifest):
    def __init__(self):
        self.sdUUID = 'a6ecac0a-5c6b-46d7-9ba5-df8b34df2d01'
        self.domaindir = '/a/b/c'
        self.mountpoint = '/a/b'
        self._metadata = {}
        self.__class__._classmethod_calls = []

    @classmethod
    def record_classmethod_call(cls, fn, args):
        cls._classmethod_calls.append((fn, args))

    @classmethod
    def get_classmethod_calls(cls):
        return cls._classmethod_calls

    @recorded
    def replaceMetadata(self, md):
        pass

    @recorded
    def getIsoDomainImagesDir(self):
        pass

    @recorded
    def getMDPath(self):
        pass

    @recorded
    def getMetaParam(self, key):
        pass

    @recorded
    def getVersion(self):
        pass

    @recorded
    def getMetadata(self):
        pass

    @recorded
    def getFormat(self):
        pass

    @recorded
    def getPools(self):
        pass

    @recorded
    def getRepoPath(self):
        pass

    @recorded
    def getStorageType(self):
        pass

    @recorded
    def getDomainRole(self):
        pass

    @recorded
    def getDomainClass(self):
        pass

    @recorded
    def isISO(self):
        pass

    @recorded
    def isBackup(self):
        pass

    @recorded
    def isData(self):
        pass

    @recorded
    def deleteImage(self, sdUUID, imgUUID, volsImgs):
        pass

    @recorded
    def getAllImages(self):
        pass

    @recorded
    def getAllVolumes(self):
        pass

    @recorded
    def getReservedId(self):
        pass

    @recorded
    def acquireHostId(self, hostId, async=False):
        pass

    @recorded
    def releaseHostId(self, hostId, async=False):
        pass

    @recorded
    def hasHostId(self, hostId):
        pass

    @recorded
    def getHostStatus(self, hostId):
        pass

    @recorded
    def acquireDomainLock(self, hostID):
        pass

    @recorded
    def releaseDomainLock(self):
        pass

    @recorded
    def inquireDomainLock(self):
        pass

    @recorded
    def hasVolumeLeases(self):
        pass

    @recorded
    def _makeDomainLock(self, domVersion):
        pass

    @recorded
    def refreshDirTree(self):
        pass

    @recorded
    def refresh(self):
        pass

    @recorded
    def validateCreateVolumeParams(self, volFormat, srcVolUUID,
                                   preallocate=None):
        pass


class FakeBlockDomainManifest(FakeDomainManifest):
    def __init__(self):
        FakeDomainManifest.__init__(self)
        self.logBlkSize = 512
        self.phyBlkSize = 512

    @recorded
    def getReadDelay(self):
        pass

    @recorded
    def getVSize(self, imgUUID, volUUID):
        pass

    @recorded
    def getVAllocSize(self, imgUUID, volUUID):
        pass

    @recorded
    def getLeasesFilePath(self):
        pass

    @recorded
    def getIdsFilePath(self):
        pass

    @recorded
    def readMetadataMapping(self):
        pass

    @classmethod
    def metaSize(cls, *args):
        cls.record_classmethod_call('metaSize', args)

    @classmethod
    def getMetaDataMapping(cls, *args):
        cls.record_classmethod_call('getMetaDataMapping', args)

    @recorded
    def resizePV(self, guid):
        pass

    @recorded
    def extend(self, devlist, force):
        pass

    @recorded
    def extendVolume(self, volumeUUID, size, isShuttingDown=None):
        pass

    @recorded
    def getVolumeClass(self):
        pass

    @recorded
    def rmDCImgDir(self, imgUUID, volsImgs):
        pass

    @recorded
    def _getImgExclusiveVols(self, imgUUID, volsImgs):
        pass

    @recorded
    @contextmanager
    def acquireVolumeMetadataSlot(self, vol_name, slotSize):
        yield


class FakeFileDomainManifest(FakeDomainManifest):
    def __init__(self):
        FakeDomainManifest.__init__(self)
        self.remotePath = 'b'

    @recorded
    def getReadDelay(self):
        pass

    @recorded
    def getVSize(self, imgUUID, volUUID):
        pass

    @recorded
    def getVAllocSize(self, imgUUID, volUUID):
        pass

    @recorded
    def getLeasesFilePath(self):
        pass

    @recorded
    def getIdsFilePath(self):
        pass

    @recorded
    def getVolumeClass(self):
        pass


class FakeBlockStorageDomain(blockSD.BlockStorageDomain):
    manifestClass = FakeBlockDomainManifest

    def __init__(self):
        self.stat = None
        self._manifest = self.manifestClass()


class FakeFileStorageDomain(fileSD.FileStorageDomain):
    manifestClass = FakeFileDomainManifest

    def __init__(self):
        self.stat = None
        self._manifest = self.manifestClass()


class FakeImageManifest(image.ImageManifest):
    def __init__(self):
        self._repoPath = '/rhev/data-center'

    @recorded
    def getImageDir(self, sdUUID, imgUUID):
        pass


class FakeImage(image.Image):
    def __init__(self):
        self._manifest = FakeImageManifest()


class FakeVolumeMetadata(volume.VolumeMetadata):
    def __init__(self):
        self.sdUUID = 'b4502284-2101-4c5c-ada0-6a196fb30315'
        self.imgUUID = 'e2a325e4-62be-4939-8145-72277c270e8e'
        self.volUUID = '6aab5eb4-2a8b-4cb7-a0b7-bc6f61de3e18'
        self.repoPath = '/rhev/data-center'
        self._imagePath = '/a/b'
        self._volumePath = '/a/b/c'
        self.voltype = None

    @recorded
    def getVolumePath(self):
        pass

    @recorded
    def getMetadataId(self):
        pass

    @recorded
    def getMetadata(self, metaId=None):
        pass

    @recorded
    def getMetaParam(self, key):
        pass


class FakeBlockVolumeMetadata(FakeVolumeMetadata):
    def __init__(self):
        super(FakeBlockVolumeMetadata, self).__init__()
        self.metaoff = None

    @recorded
    def getMetaOffset(self):
        pass


class FakeFileVolumeMetadata(FakeVolumeMetadata):
    def __init__(self):
        super(FakeFileVolumeMetadata, self).__init__()
        self.oop = 'oop'

    @recorded
    def _getMetaVolumePath(self, vol_path=None):
        pass


class FakeFileVolume(fileVolume.FileVolume):
    metadataClass = FakeFileVolumeMetadata

    def __init__(self):
        self._md = self.metadataClass()


class FakeBlockVolume(blockVolume.BlockVolume):
    metadataClass = FakeBlockVolumeMetadata

    def __init__(self):
        self._md = self.metadataClass()


class RedirectionChecker(object):
    """
    Checks whether a source class redirects method calls to a target class
    instance accessible via the 'target_name" attribute.  The target class
    methods must use the @recorded decorator.
    """
    def __init__(self, source_instance, target_name):
        self.source_instance = source_instance
        self.target_name = target_name

    def check(self, fn, args, result):
        target = getattr(self.source_instance, self.target_name)
        getattr(self.source_instance, fn)(*args)
        return target.__recording__ == result

    def check_call(self, fn, nr_args=0):
        args = tuple(range(nr_args))
        return self.check(fn, args, [(fn, args, {})])

    def check_classmethod_call(self, fn, nr_args=0):
        args = tuple(range(nr_args))
        target = getattr(self.source_instance, self.target_name)
        getattr(self.source_instance, fn)(*args)
        return target.get_classmethod_calls() == [(fn, args)]


@expandPermutations
class DomainTestMixin(object):

    @permutations([
        ['sdUUID', 'a6ecac0a-5c6b-46d7-9ba5-df8b34df2d01'],
        ['domaindir', '/a/b/c'],
        ['_metadata', {}],
        ['mountpoint', '/a/b'],
    ])
    def test_property(self, prop, val):
        self.assertEqual(getattr(self.domain, prop), val)

    def test_getrepopath(self):
        # The private method _getRepoPath in StorageDomain calls the public
        # method getRepoPath in the StorageDomainManifest.
        self.checker.check('_getRepoPath', (), [('getRepoPath', (), {})])

    def test_nonexisting_function(self):
        self.assertRaises(AttributeError, self.checker.check_call, 'foo')

    @permutations([
        # dom method, manifest method, nargs
        ['acquireClusterLock', 'acquireDomainLock', 1],
        ['releaseClusterLock', 'releaseDomainLock', 0],
        ['inquireClusterLock', 'inquireDomainLock', 0],
        ['_makeClusterLock', '_makeDomainLock', 1],
    ])
    def test_clusterlock(self, dom_method, manifest_method, nr_args):
        args = tuple(range(nr_args))
        self.checker.check(dom_method, args, [(manifest_method, args, {})])

    @permutations([
        ['getReadDelay', 0],
        ['replaceMetadata', 1],
        ['getVSize', 2],
        ['getVAllocSize', 2],
        ['getLeasesFilePath', 0],
        ['getIdsFilePath', 0],
        ['getIsoDomainImagesDir', 0],
        ['getMDPath', 0],
        ['getMetaParam', 1],
        ['getVersion', 0],
        ['getMetadata', 0],
        ['getVolumeClass', 0],
        ['getFormat', 0],
        ['getPools', 0],
        ['getStorageType', 0],
        ['getDomainRole', 0],
        ['getDomainClass', 0],
        ['isISO', 0],
        ['isBackup', 0],
        ['isData', 0],
        ['deleteImage', 3],
        ['getAllImages', 0],
        ['getAllVolumes', 0],
        ['getReservedId', 0],
        ['acquireHostId', 2],
        ['releaseHostId', 2],
        ['hasHostId', 1],
        ['getHostStatus', 1],
        ['hasVolumeLeases', 0],
        ['refreshDirTree', 0],
        ['refresh', 0],
        ['validateCreateVolumeParams', 3],
        ])
    def test_common_functions(self, fn, nargs):
        self.checker.check_call(fn, nargs)


@expandPermutations
class BlockDomainTests(DomainTestMixin, VdsmTestCase):

    def setUp(self):
        self.domain = FakeBlockStorageDomain()
        self.checker = RedirectionChecker(self.domain, '_manifest')

    def test_block_properties(self):
        self.assertEqual(512, self.domain.logBlkSize)
        self.assertEqual(512, self.domain.phyBlkSize)

    def test_acquirevolumemetadataslot(self):
        with self.domain.acquireVolumeMetadataSlot(0, 1):
            result = [('acquireVolumeMetadataSlot', (0, 1), {})]
            self.assertEqual(self.domain._manifest.__recording__, result)

    @permutations([
        ['extend', 2],
        ['resizePV', 1],
        ['readMetadataMapping', 0],
        ['extendVolume', 3],
        ['rmDCImgDir', 2],
    ])
    def test_block_functions(self, fn, nargs=0):
        self.checker.check_call(fn, nargs)

    @permutations([
        ['metaSize', 1],
        ['getMetaDataMapping', 2],
    ])
    def test_block_classmethod(self, fn, nargs=0):
        self.checker.check_classmethod_call(fn, nargs)


class FileDomainTests(DomainTestMixin, VdsmTestCase):

    def setUp(self):
        self.domain = FakeFileStorageDomain()
        self.checker = RedirectionChecker(self.domain, '_manifest')

    def test_getremotepath(self):
        self.assertEqual('b', self.domain.getRemotePath())


@expandPermutations
class ImageTest(VdsmTestCase):
    def setUp(self):
        self.image = FakeImage()
        self.checker = RedirectionChecker(self.image, '_manifest')

    def test_properties(self):
        self.assertEqual('/rhev/data-center', self.image.repoPath)

    @permutations([
        ['getImageDir', 2],
    ])
    def test_functions(self, fn, nargs):
        self.checker.check_call(fn, nargs)


@expandPermutations
class VolumeTestMixin(object):

    @permutations([
        ['sdUUID', 'b4502284-2101-4c5c-ada0-6a196fb30315'],
        ['imgUUID', 'e2a325e4-62be-4939-8145-72277c270e8e'],
        ['volUUID', '6aab5eb4-2a8b-4cb7-a0b7-bc6f61de3e18'],
        ['repoPath', '/rhev/data-center'],
        ['imagePath', '/a/b'],
        ['volumePath', '/a/b/c'],
        ['voltype', None],
        ])
    def test_property(self, prop, val):
        self.assertEqual(getattr(self.volume, prop), val)

    @permutations([
        ['getVolumePath', 0],
        ['getMetadataId', 0],
        ['getMetadata', 0],
        ['getMetaParam', 1],
        ])
    def test_functions(self, fn, nargs):
        self.checker.check_call(fn, nargs)


@expandPermutations
class BlockVolumeTests(VolumeTestMixin, VdsmTestCase):

    def setUp(self):
        self.volume = FakeBlockVolume()
        self.checker = RedirectionChecker(self.volume, '_md')

    @permutations([
        ['metaoff', None],
        ])
    def test_block_property(self, prop, val):
        self.assertEqual(getattr(self.volume, prop), val)

    @permutations([
        ['getMetaOffset', 0],
        ])
    def test_functions(self, fn, nargs):
        self.checker.check_call(fn, nargs)


@expandPermutations
class FileVolumeTests(VolumeTestMixin, VdsmTestCase):

    def setUp(self):
        self.volume = FakeFileVolume()
        self.checker = RedirectionChecker(self.volume, '_md')

    @permutations([
        ['oop', 'oop'],
        ])
    def test_file_property(self, prop, val):
        self.assertEqual(getattr(self.volume, prop), val)

    @permutations([
        ['_getMetaVolumePath', 1],
        ])
    def test_functions(self, fn, nargs):
        self.checker.check_call(fn, nargs)
