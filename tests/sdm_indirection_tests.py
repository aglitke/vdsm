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

from testlib import VdsmTestCase
from testlib import permutations, expandPermutations
from testlib import recorded

from storage import sd, blockSD, fileSD


class FakeDomainManifest(sd.StorageDomainManifest):
    def __init__(self):
        self.sdUUID = 'a6ecac0a-5c6b-46d7-9ba5-df8b34df2d01'
        self.domaindir = '/a/b/c'
        self.mountpoint = '/a/b'
        self._metadata = {}

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
    def getLeasesFilePath(self):
        pass

    @recorded
    def getIdsFilePath(self):
        pass


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


class FakeBlockStorageDomain(blockSD.BlockStorageDomain):
    def __init__(self):
        self.stat = None
        self._manifest = FakeBlockDomainManifest()


class FakeFileStorageDomain(fileSD.FileStorageDomain):
    def __init__(self):
        self.stat = None
        self._manifest = FakeFileDomainManifest()


@expandPermutations
class StorageDomainManifestTests(VdsmTestCase):
    def _get_domain(self, domType):
        if domType == 'block':
            return FakeBlockStorageDomain()
        elif domType == 'file':
            return FakeFileStorageDomain()
        else:
            raise ValueError("%s is not a valid domain type" % domType)

    def _check(self, domType, fn, args, result):
        dom = self._get_domain(domType)
        getattr(dom, fn)(*args)
        self.assertEquals(dom._manifest.__recording__, result)

    def check_call(self, domType, fn, nr_args=0):
        args = tuple(range(nr_args))
        self._check(domType, fn, args, [(fn, args, {})])

    @permutations([['block'], ['file']])
    def test_properties(self, domType):
        dom = self._get_domain(domType)
        self.assertEquals('a6ecac0a-5c6b-46d7-9ba5-df8b34df2d01', dom.sdUUID)
        self.assertEquals('/a/b/c', dom.domaindir)
        self.assertEquals({}, dom._metadata)
        self.assertEquals('/a/b', dom.mountpoint)

    @permutations([['block']])
    def test_block_properties(self, domType):
        dom = self._get_domain(domType)
        self.assertEquals(512, dom.logBlkSize)
        self.assertEquals(512, dom.phyBlkSize)

    @permutations([['file']])
    def test_getremotepath(self, domType):
        dom = self._get_domain(domType)
        self.assertEquals('b', dom.getRemotePath())

    @permutations([['block'], ['file']])
    def test_nonexisting_function(self, domType):
        self.assertRaises(AttributeError, self.check_call, domType, 'foo')

    @permutations([
        ['getReadDelay'],
        ['replaceMetadata', 1],
        ['getVSize', 2],
        ['getLeasesFilePath'],
        ['getIdsFilePath'],
        ['getIsoDomainImagesDir'],
        ['getMDPath'],
        ['getMetaParam', 1],
    ])
    def test_domain_funcs(self, fn, nargs=0):
        for domType in 'file', 'block':
            self.check_call(domType, fn, nargs)

    @permutations([
        ['getVAllocSize', 2]
    ])
    def test_file_domain_funcs(self, fn, nargs=0):
        self.check_call('file', fn, nargs)

    @permutations([['block']])
    def test_block_getvallocsize(self, domType):
        # On block domains, this call redirects to the getVSize function
        recording = [('getVSize', (0, 1), {})]
        self._check(domType, 'getVAllocSize', (0, 1), recording)
