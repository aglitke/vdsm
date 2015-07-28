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
import string
import random
from copy import deepcopy

from storage import lvm as real_lvm


class FakeLVM(object):
    _PV_SIZE = 10 << 30             # We pretend all PVs are 10G in size
    _PV_PE_SIZE = 128 << 20         # Found via inspection of real environment
    _PV_UNUSABLE = _PV_PE_SIZE * 3  # 2 PE for metadata + 1 PE to hold a header

    def __init__(self, root):
        self.root = root
        os.mkdir(os.path.join(self.root, 'dev'))
        self.pvmd = {}
        self.vgmd = {}
        self.lvmd = {}

    def createVG(self, vgName, devices, initialTag, metadataSize,
                 extentsize=134217728, force=False):
        devices = [_fqpvname(dev) for dev in devices]
        for dev in devices:
            self._create_pv(dev, vgName, self._PV_SIZE)
        extent_count = self._calc_vg_pe_count(vgName)
        size = extent_count * self._PV_PE_SIZE

        vg_attr = dict(permission='w',
                       resizeable='z',
                       exported='-',
                       partial='-',
                       allocation='n',
                       clustered='-')
        vg_md = dict(uuid=fake_lvm_uuid(),
                     name=vgName,
                     attr=vg_attr,
                     size=size,
                     free=size,
                     extent_size=extentsize,
                     extent_count=extent_count,
                     free_count=extent_count,
                     tags=[initialTag],
                     vg_mda_size=metadataSize,
                     vg_mda_free=metadataSize,
                     lv_count=0,
                     pv_count=len(devices),
                     pv_name=tuple(devices),
                     writeable=True,
                     partial='OK')
        self.vgmd[vgName] = vg_md

    def extendVG(self, vgName, devices, force):
        md = self.vgmd[vgName]
        old_pe_count = self._calc_vg_pe_count(vgName)
        devices = [_fqpvname(dev) for dev in devices]
        for dev in devices:
            self._create_pv(dev, vgName, self._PV_SIZE)
        nr_pe_added = self._calc_vg_pe_count(vgName) - old_pe_count
        md['pv_count'] += len(devices)
        md['pv_name'] = tuple(md['pv_name'] + tuple(devices))
        md['size'] += nr_pe_added * self._PV_PE_SIZE
        md['free'] += nr_pe_added * self._PV_PE_SIZE
        md['extent_count'] += nr_pe_added
        md['free_count'] += nr_pe_added

    def createLV(self, vgName, lvName, size, activate=True, contiguous=False,
                 initialTags=[]):
        lv_attr = dict(voltype='-',
                       permission='w',
                       allocations='i',
                       fixedminor='-',
                       state='a',
                       devopen='-',
                       target='-',
                       zero='-')
        lv_md = dict(uuid=fake_lvm_uuid(),
                     name=lvName,
                     vg_name=vgName,
                     attr=lv_attr,
                     size=str(size),
                     seg_start_pe='0',
                     devices='',
                     tags=(),
                     writeable=True,
                     opened=False,
                     active=True)

        vg_dict = self.lvmd.setdefault(vgName, {})
        vg_dict[lvName] = lv_md
        self.vgmd[vgName]['lv_count'] += 1

    def extendLV(self, vgName, lvName, size):
        # TODO: Update the allocated physical extents
        self.lvmd[vgName][lvName]['size'] = size

    def lvPath(self, vgName, lvName):
        return os.path.join(self.root, "dev", vgName, lvName)

    def getPV(self, pvName):
        md = deepcopy(self.pvmd[pvName])
        vg_name = md['vg_name']
        md['vg_uuid'] = self.vgmd[vg_name]['uuid']
        return real_lvm.PV(**md)

    def getVG(self, vgName):
        vg_md = deepcopy(self.vgmd[vgName])
        vg_attr = real_lvm.VG_ATTR(**vg_md['attr'])
        vg_md['attr'] = vg_attr
        return real_lvm.VG(**vg_md)

    def getLV(self, vgName, lvName):
        lv_md = deepcopy(self.lvmd[vgName][lvName])
        lv_attr = real_lvm.LV_ATTR(**lv_md['attr'])
        lv_md['attr'] = lv_attr
        return real_lvm.LV(**lv_md)

    def getFirstExt(self, vg, lv):
        """
        Based on the following example output from real LVM:
        # lvs -o name,devices
        LV                                   Devices
        4c705e28-ad99-46c2-b1eb-43fd8501ea97 /dev/mapper/1IET_00010004(31)
        ids                                  /dev/mapper/1IET_00010004(21)
        inbox                                /dev/mapper/1IET_00010004(22)
        leases                               /dev/mapper/1IET_00010004(5)
        master                               /dev/mapper/1IET_00010004(23)
        metadata                             /dev/mapper/1IET_00010004(0)
        outbox                               /dev/mapper/1IET_00010004(4)
        """
        return self.getLV(vg, lv).devices.strip(" )").split("(")

    def listPVNames(self, vgName):
        return self.getVG(vgName).pv_name

    def fake_lv_symlink_create(self, vg_name, lv_name):
        volpath = self.lvPath(vg_name, lv_name)
        os.makedirs(os.path.dirname(volpath))
        with open(volpath, "w") as f:
            f.truncate(int(self.lvmd[vg_name][lv_name]['size']))

    def _create_pv(self, pv_name, vg_name, size):
        """
        pe_start is set based on the following example from real LVM:
        # vgs --units b -o pv_name,pe_start,pv_pe_count,pv_pe_alloc_count,\
        pv_size 2e133cd6-bfea-4655-ae6c-dc51a38bb7b9
        PV                        1st PE     PE  Alloc PSize
        /dev/mapper/1IET_00070001 135266304B 397    65 53284438016B
        /dev/mapper/1IET_00070002 135266304B 397     0 53284438016B

        This is an example vg with 2 pvs and 10 lvs.
        """
        pe_start = 135266304
        pe_count = (size - self._PV_UNUSABLE) / self._PV_PE_SIZE
        pv_md = dict(uuid=fake_lvm_uuid(),
                     name=pv_name,
                     guid=os.path.basename(pv_name),
                     size=self._PV_SIZE,
                     vg_name=vg_name,
                     vg_uuid=None,
                     pe_start=pe_start,
                     pe_count=pe_count,
                     pe_alloc_count=0,
                     mda_count=None,
                     dev_size=self._PV_SIZE)
        self.pvmd[pv_name] = pv_md

    def fake_pv_update_size(self, pv_name, new_size):
        pvmd = self.pvmd[pv_name]
        size_added = new_size - pvmd['size']
        extents_added = size_added / self._PV_PE_SIZE
        pvmd['size'] += size_added
        pvmd['dev_size'] += size_added
        pvmd['pe_count'] += extents_added

        vg_name = pvmd['vg_name']
        vgmd = self.vgmd[vg_name]
        vgmd['size'] += size_added
        vgmd['free'] += size_added
        vgmd['extent_count'] += extents_added
        vgmd['free_count'] += extents_added

    def resizePV(self, vg_name, guid):
        # There is nothing to do since fake_pv_update_size() took care of
        # the LVM synchronization.
        pass

    def _calc_vg_pe_count(self, vg_name):
        return sum(pv["pe_count"] for pv in self.pvmd.values()
                   if pv["vg_name"] == vg_name)


_fqpvname = real_lvm._fqpvname


def fake_lvm_uuid():
    chars = string.ascii_letters + string.digits

    def part(size):
        return ''.join(random.choice(chars) for _ in range(size))
    return '-'.join(part(size) for size in [6, 4, 4, 4, 4, 6])
