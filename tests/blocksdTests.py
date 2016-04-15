#
# Copyright 2014-2015 Red Hat, Inc.
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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301  USA
#
# Refer to the README and COPYING files for full details of the license
#

import collections
import os

from monkeypatch import MonkeyPatch
from testlib import VdsmTestCase as TestCaseBase

from storage import blockSD
from storage import lvm
from vdsm import constants

# Make it easy to test the values we care about
VG = collections.namedtuple("VG", ['vg_mda_size', 'vg_mda_free'])

TESTDIR = os.path.dirname(__file__)


class MatadataValidityTests(TestCaseBase):

    MIN_MD_SIZE = blockSD.VG_METADATASIZE * constants.MEGAB / 2
    MIN_MD_FREE = MIN_MD_SIZE * blockSD.VG_MDA_MIN_THRESHOLD

    def test_valid_ok(self):
        vg = VG(self.MIN_MD_SIZE, self.MIN_MD_FREE)
        self.assertEquals(True, blockSD.metadataValidity(vg)['mdavalid'])

    def test_valid_bad(self):
        vg = VG(self.MIN_MD_SIZE - 1, self.MIN_MD_FREE)
        self.assertEquals(False, blockSD.metadataValidity(vg)['mdavalid'])

    def test_threshold_ok(self):
        vg = VG(self.MIN_MD_SIZE, self.MIN_MD_FREE + 1)
        self.assertEquals(True, blockSD.metadataValidity(vg)['mdathreshold'])

    def test_threshold_bad(self):
        vg = VG(self.MIN_MD_SIZE, self.MIN_MD_FREE)
        self.assertEquals(False, blockSD.metadataValidity(vg)['mdathreshold'])


def fakeGetLV(vgName):
    """ This function returns lvs output in lvm.getLV() format.

    Input file name: lvs_<sdName>.out
    Input file should be the output of:
    lvs --noheadings --units b --nosuffix --separator '|' \
        -o uuid,name,vg_name,attr,size,seg_start_pe,devices,tags <sdName>

    """
    # TODO: simplify by returning fake lvs instead of parsing real lvs output.
    lvs_output = os.path.join(TESTDIR, 'lvs_%s.out' % vgName)
    lvs = []
    with open(lvs_output) as f:
        for line in f:
            fields = [field.strip() for field in line.split(lvm.SEPARATOR)]
            lvs.append(lvm.makeLV(*fields))
    return lvs


class GetAllVolumesTests(TestCaseBase):
    # TODO: add more tests, see fileSDTests.py

    @MonkeyPatch(lvm, 'getLV', fakeGetLV)
    def test_volumes_count(self):
        sdName = "3386c6f2-926f-42c4-839c-38287fac8998"
        allVols = blockSD.getAllVolumes(sdName)
        self.assertEqual(len(allVols), 23)

    @MonkeyPatch(lvm, 'getLV', fakeGetLV)
    def test_missing_tags(self):
        sdName = "f9e55e18-67c4-4377-8e39-5833ca422bef"
        allVols = blockSD.getAllVolumes(sdName)
        self.assertEqual(len(allVols), 1)
