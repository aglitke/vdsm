#
# Copyright 2016 Red Hat, Inc.
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

import uuid

from testlib import VdsmTestCase
from sdmtestlib import wait_for_job

from vdsm import jobs
from vdsm import exception
from vdsm.storage import exception as se

from storage.sdm.api import base
from storage.sdm.api import types
from storage.threadLocal import vars


def _get_create_vol_info():
    parent = dict(img_id=str(uuid.uuid4()), vol_id=str(uuid.uuid4()))
    return dict(sd_id=str(uuid.uuid4()), img_id=str(uuid.uuid4()),
                vol_id=str(uuid.uuid4()), virtual_size=2048,
                vol_format='RAW', disk_type='SYSTEM',
                description='test vol', parent=parent, initial_size='0')


class TypesTests(VdsmTestCase):

    def test_required(self):
        params = {'foo': 'bar'}
        self.assertEqual('bar', types._required(params, 'foo'))

    def test_required_missing(self):
        params = {}
        self.assertRaises(exception.MissingParameter,
                          types._required, params, 'missing')

    def test_enum(self):
        params = {'foo': 3}
        self.assertEqual(3, types._enum(params, 'foo', [1, 2, 3]))

    def test_invalid_enum_value(self):
        params = {'foo': -1}
        self.assertRaises(se.InvalidParameterException,
                          types._enum, params, 'foo', [1, 2, 3])

    def test_createvolumeinfo_parent_missing(self):
        params = _get_create_vol_info()
        del params['parent']
        self.assertIsNone(types.CreateVolumeInfo(params).parent)

    def test_createvolumeinfo_parent_none(self):
        params = _get_create_vol_info()
        params['parent'] = None
        self.assertIsNone(types.CreateVolumeInfo(params).parent)

    def test_createvolumeinfo_parent_empty(self):
        params = _get_create_vol_info()
        params['parent'] = {}
        self.assertIsNone(types.CreateVolumeInfo(params).parent)


class ApiBaseTests(VdsmTestCase):

    def run_job(self, job):
        self.assertEqual(jobs.STATUS.PENDING, job.status)
        self.assertIsNone(getattr(vars, 'job_id', None))
        job.run()
        wait_for_job(job)
        self.assertIsNone(getattr(vars, 'job_id', None))

    def test_states(self):
        job = TestingJob()
        self.run_job(job)
        self.assertEqual(jobs.STATUS.DONE, job.status)

    def test_default_exception(self):
        message = "testing failure"
        job = TestingJob(Exception(message))
        self.run_job(job)
        self.assertEqual(jobs.STATUS.FAILED, job.status)
        self.assertIsInstance(job.error, exception.GeneralException)
        self.assertIn(message, str(job.error))

    def test_vdsm_exception(self):
        job = TestingJob(exception.VdsmException())
        self.run_job(job)
        self.assertEqual(jobs.STATUS.FAILED, job.status)
        self.assertIsInstance(job.error, exception.VdsmException)


class TestingJob(base.Job):

    def __init__(self, exception=None):
        job_id = str(uuid.uuid4())
        super(TestingJob, self).__init__(job_id, 'testing_job', 'host_id')
        self.exception = exception

    def _run(self):
        assert(self.status == jobs.STATUS.RUNNING)
        assert(vars.job_id == self.id)
        if self.exception:
            raise self.exception
