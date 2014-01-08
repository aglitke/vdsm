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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
#
# Refer to the README and COPYING files for full details of the license
#

import os
from glob import iglob, glob
import logging
import threading
import errno
import uuid
import codecs
from contextlib import nested
from functools import partial
from weakref import proxy

from imageRepository.formatConverter import DefaultFormatConverter

from vdsm import constants, utils
import storage_mailbox
import blockSD
import fileSD
import sd
import misc
import fileUtils
from vdsm.config import config
from sdc import sdCache
import storage_exception as se
from persistentDict import DictValidator, unicodeEncoder, unicodeDecoder
from remoteFileHandler import Timeout
from securable import Securable, unsecured
import image
from resourceFactories import IMAGE_NAMESPACE
from storageConstants import STORAGE
import resourceManager as rm
import volume
import mount

POOL_MASTER_DOMAIN = 'mastersd'

MAX_POOL_DESCRIPTION_SIZE = 50

PMDK_DOMAINS = "POOL_DOMAINS"
PMDK_POOL_DESCRIPTION = "POOL_DESCRIPTION"
PMDK_LVER = "POOL_SPM_LVER"
PMDK_SPM_ID = "POOL_SPM_ID"
PMDK_MASTER_VER = "MASTER_VERSION"

rmanager = rm.ResourceManager.getInstance()

SPM_ACQUIRED = 'SPM'
SPM_CONTEND = 'Contend'
SPM_FREE = 'Free'
SPM_ID_FREE = -1
LVER_INVALID = -1


def domainListEncoder(domDict):
    domains = ','.join(['%s:%s' % (k, v) for k, v in domDict.iteritems()])
    return domains


def domainListDecoder(s):
    domList = {}
    if not s:
        return domList
    for domDecl in s.split(","):
        k, v = domDecl.split(':')
        domList[k.strip("'")] = v.strip("'").capitalize()
    return domList

SP_MD_FIELDS = {
    # Key          dec,  enc
    PMDK_DOMAINS: (domainListDecoder, domainListEncoder),
    PMDK_POOL_DESCRIPTION: (unicodeDecoder, unicodeEncoder),
    PMDK_LVER: (int, str),
    PMDK_SPM_ID: (int, str),
    PMDK_MASTER_VER: (int, str)
}

# Calculate how many domains can be in the pool before overflowing the Metadata
MAX_DOMAINS = blockSD.SD_METADATA_SIZE - blockSD.METADATA_BASE_SIZE
MAX_DOMAINS -= MAX_POOL_DESCRIPTION_SIZE + sd.MAX_DOMAIN_DESCRIPTION_SIZE
MAX_DOMAINS -= blockSD.PVS_METADATA_SIZE
MAX_DOMAINS /= 48


class StoragePool(Securable):
    '''
    StoragePool object should be relatively cheap to construct. It should defer
    any heavy lifting activities until the time it is really needed.
    '''

    log = logging.getLogger('Storage.StoragePool')
    storage_repository = config.get('irs', 'repository')
    lvExtendPolicy = config.get('irs', 'vol_extend_policy')

    def __init__(self, spUUID, domainMonitor, taskManager):
        Securable.__init__(self)

        self._formatConverter = DefaultFormatConverter()
        self._domainsToUpgrade = []
        self.lock = threading.RLock()
        self._setUnsafe()
        self.spUUID = str(spUUID)
        self.poolPath = os.path.join(self.storage_repository, self.spUUID)
        self.id = SPM_ID_FREE
        self.taskMng = taskManager
        self.hsmMailer = None
        self.spmMailer = None
        self.masterDomain = None
        self.spmRole = SPM_FREE
        self.domainMonitor = domainMonitor
        self._upgradeCallback = partial(StoragePool._upgradePoolDomain,
                                        proxy(self))

    @unsecured
    def getSpmStatus(self):
        poolMeta = self._getPoolMD(self.masterDomain)

        # if we claim that we were the SPM (but we're currently not) we
        # have to make sure that we're not returning stale data
        if (poolMeta[PMDK_SPM_ID] == self.id
                and not self.spmRole == SPM_ACQUIRED):
            self.invalidateMetadata()
            poolMeta = self._getPoolMD(self.masterDomain)

        return poolMeta[PMDK_LVER], poolMeta[PMDK_SPM_ID]

    def setSpmStatus(self, lVer=None, spmId=None):
        self.invalidateMetadata()
        metaParams = dict(filter(lambda (k, v): v is not None,
                          ((PMDK_LVER, lVer), (PMDK_SPM_ID, spmId))))
        self._metadata.update(metaParams)

    @unsecured
    def getDomainsMap(self):
        # The assumption is that whenever the storage pool metadata changes
        # the HSM hosts will receive refreshStoragePool (and the metadata will
        # be invalidated). So the invalidation in this method may be redundant
        # or it was introduced to handle negative flows (missed refresh call).
        # Anyway I think that we could get rid of this in the future, provided
        # that the engine handles/resends failed refreshStoragePool calls.
        self.invalidateMetadata()
        return self.getMetaParam(PMDK_DOMAINS)

    def setDomainsMap(self, domains):
        self.setMetaParam(PMDK_DOMAINS, domains)

    @unsecured
    def forceFreeSpm(self):
        # DO NOT USE, STUPID, HERE ONLY FOR BC
        # TODO: SCSI Fence the 'lastOwner'
        self.setSpmStatus(LVER_INVALID, SPM_ID_FREE, __securityOverride=True)
        self.spmRole = SPM_FREE

    def _upgradePoolDomain(self, sdUUID, isValid):
        # This method is called everytime the onDomainStateChange
        # event is emitted, this event is emitted even when a domain goes
        # INVALID if this happens there is nothing for us to do no matter what
        # the domain is
        if not isValid:
            return

        domain = sdCache.produce(sdUUID)
        if sdUUID not in self._domainsToUpgrade:
            return

        self.log.debug("Preparing to upgrade domain %s", sdUUID)

        try:
            #Assumed that the domain can be attached only to one pool
            targetDomVersion = self.masterDomain.getVersion()
        except:
            self.log.error("Error while preparing domain `%s` upgrade", sdUUID,
                           exc_info=True)
            return

        with rmanager.acquireResource(STORAGE, "upgrade_" + sdUUID,
                                      rm.LockType.exclusive):
            with rmanager.acquireResource(STORAGE, sdUUID,
                                          rm.LockType.exclusive):
                if sdUUID not in self._domainsToUpgrade:
                    return

                # This can never be the master
                # Non data domain should not be upgraded
                domClass = domain.getDomainClass()
                if domClass != sd.DATA_DOMAIN:
                    self.log.debug("Domain `%s` is not a data domain it is an "
                                   "%s domain, not upgrading", sdUUID,
                                   domClass)
                else:
                    domain.invalidateMetadata()
                    domVersion = domain.getVersion()
                    if domVersion > targetDomVersion:
                        self.log.critical("Found a domain with a more advanced"
                                          " version then the master domain")
                    elif domVersion < targetDomVersion:
                        try:
                            self._convertDomain(domain, str(targetDomVersion))
                        except:
                            self.log.warn("Could not upgrade domain `%s`",
                                          sdUUID, exc_info=True)
                            return

                self._domainsToUpgrade.remove(sdUUID)
                if len(self._domainsToUpgrade) == 0:
                    self.log.debug("All domains are upgraded, unregistering "
                                   "from state change event")
                    try:
                        self.domainMonitor.onDomainStateChange.\
                            unregister(self._upgradeCallback)
                    except KeyError:
                        pass

    def _updateDomainsRole(self):
        for sdUUID in self.getDomains(activeOnly=True):
            if sdUUID == self.masterDomain.sdUUID:
                continue

            domain = sdCache.produce(sdUUID)
            if domain.getDomainRole() == sd.REGULAR_DOMAIN:
                continue

            self.setDomainRegularRole(domain)

    @unsecured
    def startSpm(self, prevID, prevLVER, maxHostID, expectedDomVersion=None):
        """
        Starts the SPM functionality.

        :param spUUID: The UUID of the storage pool you want to manage with the
                       SPM.
        :type spUUID: UUID
        :param prevID: obsolete
        :param prevLVER: obsolete
        :param maxHostID: The maximun ID of the host.?
        :type maxHostID: int

        .. note::
            if the SPM is already started the function will fail silently.

        :raises: :exc:`storage_exception.OperationInProgress` if called during
                       an already running connection attempt. (makes the fact
                       that it fails silently does not matter very much).
        """
        with self.lock:
            if self.spmRole == SPM_ACQUIRED:
                return True
            # Since we added the lock the following should NEVER happen
            if self.spmRole == SPM_CONTEND:
                raise se.OperationInProgress("spm start %s" % self.spUUID)

            self.updateMonitoringThreads()
            oldlver, oldid = self.getSpmStatus()
            masterDomVersion = self.getVersion()
            # If no specific domain version was specified use current master
            # domain version
            if expectedDomVersion is None:
                expectedDomVersion = masterDomVersion

            if masterDomVersion > expectedDomVersion:
                raise se.CurrentVersionTooAdvancedError(
                    self.masterDomain.sdUUID, curVer=masterDomVersion,
                    expVer=expectedDomVersion)

            if int(oldlver) != int(prevLVER) or int(oldid) != int(prevID):
                self.log.info("expected previd:%s lver:%s got request for "
                              "previd:%s lver:%s" %
                              (oldid, oldlver, prevID, prevLVER))

            self.spmRole = SPM_CONTEND

            try:
                # Forcing to acquire the host id (if it's not acquired already)
                self.masterDomain.acquireHostId(self.id)
                self.masterDomain.acquireClusterLock(self.id)
            except:
                self.spmRole = SPM_FREE
                raise

            self.log.debug("spm lock acquired successfully")

            try:
                self.lver = int(oldlver) + 1

                self.setSpmStatus(self.lver, self.id, __securityOverride=True)
                self._maxHostID = maxHostID

                # Upgrade the master domain now if needed
                self._upgradePool(expectedDomVersion, __securityOverride=True)

                self.masterDomain.mountMaster()
                self.masterDomain.createMasterTree()
                self.tasksDir = os.path.join(self.poolPath, POOL_MASTER_DOMAIN,
                                             sd.MASTER_FS_DIR, sd.TASKS_DIR)

                try:
                    # Make sure backup domain is active
                    self.checkBackupDomain(__securityOverride=True)
                except Exception:
                    self.log.error("Backup domain validation failed",
                                   exc_info=True)

                self.taskMng.loadDumpedTasks(self.tasksDir)

                self.spmRole = SPM_ACQUIRED

                # Once setSafe completes we are running as SPM
                self._setSafe()
                self._updateDomainsRole()

                # Mailbox issues SPM commands, therefore we start it AFTER spm
                # commands are allowed to run to prevent a race between the
                # mailbox and the "self._setSafe() call"

                # Create mailbox if SAN pool (currently not needed on nas)
                # FIXME: Once pool contains mixed type domains (NFS + Block)
                #        the mailbox will have to be created if there is an
                #        active block domain in the pool or once one is
                #        activated

                #FIXME : Use a system wide grouping mechanism
                if self.masterDomain.requiresMailbox and \
                        self.lvExtendPolicy == "ON":
                    self.spmMailer = storage_mailbox.SPM_MailMonitor(self,
                                                                     maxHostID)
                    self.spmMailer.registerMessageType('xtnd', partial(
                        storage_mailbox.SPM_Extend_Message.processRequest,
                        self))
                    self.log.debug("SPM mailbox ready for pool %s on master "
                                   "domain %s", self.spUUID,
                                   self.masterDomain.sdUUID)
                else:
                    self.spmMailer = None

                # Restore tasks is last because tasks are spm ops (spm has to
                # be started)
                self.taskMng.recoverDumpedTasks()

                self.log.debug("ended.")

            except Exception as e:
                self.log.error("Unexpected error", exc_info=True)
                self.log.error("failed: %s" % str(e))
                self.stopSpm(force=True, __securityOverride=True)
                raise

    @unsecured
    def _shutDownUpgrade(self):
        self.log.debug("Shutting down upgrade process")
        with rmanager.acquireResource(STORAGE, "upgrade_" + self.spUUID,
                                      rm.LockType.exclusive):
            domains = self._domainsToUpgrade[:]
            try:
                self.domainMonitor.onDomainStateChange.unregister(
                    self._upgradeCallback)
            except KeyError:
                pass
            requests = []

            def cancelUpgrade(sdUUID, req, res):
                try:
                    self._domainsToUpgrade.remove(sdUUID)
                except ValueError:
                    pass

                res.release()

            for sdUUID in domains:
                req = rmanager.registerResource(STORAGE, "upgrade_" + sdUUID,
                                                rm.LockType.exclusive,
                                                partial(cancelUpgrade, sdUUID))
                requests.append(req)

            for req in requests:
                req.wait()

    @classmethod
    def cleanupMasterMount(cls):
        """
        Check whether there are any dangling master file systems still mounted
        and unmount them if found.
        """
        masters = os.path.join(cls.storage_repository, sd.DOMAIN_MNT_POINT,
                               sd.BLOCKSD_DIR, "*", sd.MASTER_FS_DIR)
        for master in glob(masters):
            if mount.isMounted(master):
                cls.log.debug("unmounting %s", master)
                try:
                    blockSD.BlockStorageDomain.doUnmountMaster(master)
                except se.StorageDomainMasterUnmountError as e:
                    utils.panic("unmount %s failed - %s" % (master, e))
            else:
                cls.log.debug("master `%s` is not mounted, skipping", master)

    def stopSpm(self, force=False):
        with self.lock:
            if not force and self.spmRole == SPM_FREE:
                return True

            self._shutDownUpgrade()
            self._setUnsafe()

            stopFailed = False

            try:
                self.cleanupMasterMount()
            except:
                # If unmounting fails the vdsm panics.
                stopFailed = True

            try:
                if self.spmMailer:
                    self.spmMailer.stop()
            except:
                # Here we are just begin polite.
                # SPM will also clean this on start up.
                pass

            if not stopFailed:
                try:
                    self.setSpmStatus(spmId=SPM_ID_FREE,
                                      __securityOverride=True)
                except:
                    pass  # The system can handle this inconsistency

            try:
                self.masterDomain.releaseClusterLock()
            except:
                stopFailed = True

            if stopFailed:
                utils.panic("Unrecoverable errors during SPM stop process.")

            self.spmRole = SPM_FREE

    def _upgradePool(self, targetDomVersion):
        with rmanager.acquireResource(STORAGE, "upgrade_" + self.spUUID,
                                      rm.LockType.exclusive):
            if len(self._domainsToUpgrade) > 0:
                raise se.PoolUpgradeInProgress(self.spUUID)

            sd.validateDomainVersion(targetDomVersion)
            self.log.info("Trying to upgrade master domain `%s`",
                          self.masterDomain.sdUUID)
            with rmanager.acquireResource(STORAGE, self.masterDomain.sdUUID,
                                          rm.LockType.exclusive):
                self._convertDomain(self.masterDomain, str(targetDomVersion))

            self.log.debug("Marking all domains for upgrade")
            self._domainsToUpgrade = self.getDomains(activeOnly=True).keys()
            try:
                self._domainsToUpgrade.remove(self.masterDomain.sdUUID)
            except ValueError:
                pass

            self.log.debug("Registering with state change event")
            self.domainMonitor.onDomainStateChange.register(
                self._upgradeCallback)
            self.log.debug("Running initial domain upgrade threads")
            for sdUUID in self._domainsToUpgrade:
                threading.Thread(target=self._upgradeCallback,
                                 args=(sdUUID, True),
                                 kwargs={"__securityOverride": True}).start()

    @unsecured
    def __createMailboxMonitor(self):
        if self.hsmMailer:
            return

        if self.masterDomain.requiresMailbox and self.lvExtendPolicy == "ON":
            self.hsmMailer = storage_mailbox.HSM_Mailbox(self.id, self.spUUID)
            self.log.debug("HSM mailbox ready for pool %s on master "
                           "domain %s", self.spUUID, self.masterDomain.sdUUID)

    @unsecured
    def __cleanupDomains(self, domlist, msdUUID, masterVersion):
        """
        Clean up domains after failed Storage Pool creation
        domlist - comma separated list of sdUUIDs
        """
        # Go through all the domains and detach them from the pool
        # Since something went wrong (otherwise why would we be cleaning
        # the mess up?) do not expect all the domains to exist
        for sdUUID in domlist:
            try:
                self.detachSD(sdUUID)
            except Exception:
                self.log.error("Domain %s detach from MSD %s Ver %s failed.",
                               sdUUID, msdUUID, masterVersion, exc_info=True)
        # Cleanup links to domains under /rhev/datacenter/poolName
        self.refresh(msdUUID, masterVersion)

    @unsecured
    def getMasterVersion(self):
        return self.getMetaParam(PMDK_MASTER_VER)

    def setDomainRegularRole(self, domain):
        poolMetadata = self._getPoolMD(domain)
        # TODO: consider to remove the transaction (and this method as well)
        # since setting the version to 0 may be useless.
        with poolMetadata.transaction():
            poolMetadata[PMDK_MASTER_VER] = 0
            domain.changeRole(sd.REGULAR_DOMAIN)

    # TODO: Remove or rename this function.
    def validatePoolSD(self, sdUUID):
        if sdUUID not in self.getDomains():
            raise se.StorageDomainNotMemberOfPool(self.spUUID, sdUUID)
        return True

    def validateAttachedDomain(self, dom):
        """
        Avoid handling domains if not owned by pool.
        """
        self.validatePoolSD(dom.sdUUID)
        if self.spUUID not in dom.getPools():
            dom.invalidateMetadata()
            if self.spUUID not in dom.getPools():
                raise se.StorageDomainNotInPool(self.spUUID, dom.sdUUID)
        return True

    @unsecured
    def getMaximumSupportedDomains(self):
        msdInfo = self.masterDomain.getInfo()
        msdType = sd.name2type(msdInfo["type"])
        msdVersion = int(msdInfo["version"])
        if msdType in sd.BLOCK_DOMAIN_TYPES and \
                msdVersion in blockSD.VERS_METADATA_LV:
            return MAX_DOMAINS
        else:
            return config.getint("irs", "maximum_domains_in_pool")

    @unsecured
    def _acquireTemporaryClusterLock(self, msdUUID, leaseParams):
        try:
            # Master domain is unattached and all changes to unattached domains
            # must be performed under storage lock
            msd = sdCache.produce(msdUUID)

            # As we are just creating the pool then the host doesn't have an
            # assigned id for this pool
            self.id = msd.getReservedId()

            msd.changeLeaseParams(leaseParams)

            msd.acquireHostId(self.id)

            try:
                msd.acquireClusterLock(self.id)
            except:
                msd.releaseHostId(self.id)
                raise
        except:
            self.id = SPM_ID_FREE
            raise

    @unsecured
    def _releaseTemporaryClusterLock(self, msdUUID):
        msd = sdCache.produce(msdUUID)
        try:
            msd.releaseClusterLock()
        finally:
            msd.releaseHostId(self.id)
        self.id = SPM_ID_FREE

    @unsecured
    def create(self, poolName, msdUUID, domList, masterVersion, leaseParams):
        """
        Create new storage pool with single/multiple image data domain.
        The command will create new storage pool meta-data attach each
        storage domain to that storage pool.
        At least one data (images) domain must be provided
         'poolName' - storage pool name
         'msdUUID' - master domain of this pool (one of domList)
         'domList' - list of domains (i.e sdUUID,sdUUID,...,sdUUID)
        """
        self.log.info("spUUID=%s poolName=%s master_sd=%s domList=%s "
                      "masterVersion=%s %s", self.spUUID, poolName, msdUUID,
                      domList, masterVersion, leaseParams)

        if msdUUID not in domList:
            raise se.InvalidParameterException("masterDomain", msdUUID)

        # Check the domains before pool creation
        domains = []
        msdVersion = None
        for sdUUID in domList:
            try:
                domain = sdCache.produce(sdUUID)
                domain.validate()
                if sdUUID == msdUUID:
                    msd = domain
                    msdVersion = msd.getVersion()
            except se.StorageException:
                self.log.error("Unexpected error", exc_info=True)
                raise se.StorageDomainAccessError(sdUUID)

            # Validate unattached domains
            if not domain.isISO():
                domain.invalidateMetadata()
                spUUIDs = domain.getPools()
                # Non ISO domains have only 1 pool
                if len(spUUIDs) > 0:
                    raise se.StorageDomainAlreadyAttached(spUUIDs[0], sdUUID)
            domains.append(domain)

        for domain in domains:
            if domain.isData() and (domain.getVersion() != msdVersion):
                raise se.MixedSDVersionError(domain.sdUUID,
                                             domain.getVersion(), msd.sdUUID,
                                             msdVersion)

        fileUtils.createdir(self.poolPath)
        self._acquireTemporaryClusterLock(msdUUID, leaseParams)

        try:
            self._setSafe()
            # Mark 'master' domain
            # We should do it before actually attaching this domain to the pool
            # During 'master' marking we create pool metadata and each attached
            # domain should register there
            self.createMaster(poolName, msd, masterVersion, leaseParams)
            self.__rebuild(msdUUID=msdUUID, masterVersion=masterVersion)
            # Attach storage domains to the storage pool
            # Since we are creating the pool then attach is done from the hsm
            # and not the spm therefore we must manually take the master domain
            # lock
            # TBD: create will receive only master domain and further attaches
            #      should be done under SPM

            # Master domain was already attached (in createMaster),
            # no need to reattach
            for sdUUID in domList:
                # No need to attach the master
                if sdUUID != msdUUID:
                    self.attachSD(sdUUID)
        except Exception:
            self.log.error("Create pool %s canceled ", poolName, exc_info=True)
            try:
                fileUtils.cleanupdir(self.poolPath)
                self.__cleanupDomains(domList, msdUUID, masterVersion)
            except:
                self.log.error("Cleanup failed due to an unexpected error",
                               exc_info=True)
            raise
        finally:
            self._setUnsafe()

            self._releaseTemporaryClusterLock(msdUUID)
            self.stopMonitoringDomains()

        return True

    @unsecured
    def connect(self, hostID, msdUUID, masterVersion):
        """
        Connect a Host to a specific storage pool.

        Caller must acquire resource Storage.spUUID so that this method would
        never be called twice concurrently.
        """
        self.log.info("Connect host #%s to the storage pool %s with master "
                      "domain: %s (ver = %s)" %
                      (hostID, self.spUUID, msdUUID, masterVersion))

        self.id = hostID
        # Make sure SDCache doesn't have stale data (it can be in case of FC)
        sdCache.invalidateStorage()
        sdCache.refresh()
        # Rebuild whole Pool
        self.__rebuild(msdUUID=msdUUID, masterVersion=masterVersion)
        self.__createMailboxMonitor()

        return True

    @unsecured
    def stopMonitoringDomains(self):
        for sdUUID in self.getDomains():
            self.domainMonitor.stopMonitoring(sdUUID)
        return True

    @unsecured
    def disconnect(self):
        """
        Disconnect a Host from specific storage pool.

        Caller must acquire resource Storage.spUUID so that this method would
        never be called twice concurrently.
        """
        self.log.info("Disconnect from the storage pool %s", self.spUUID)

        self.id = SPM_ID_FREE

        if self.hsmMailer:
            self.hsmMailer.stop()
            self.hsmMailer = None

        # Remove all links
        if os.path.exists(self.poolPath):
            fileUtils.cleanupdir(self.poolPath)

        self.stopMonitoringDomains()
        return True

    @unsecured
    def createMaster(self, poolName, domain, masterVersion, leaseParams):
        """
        Create a fresh master file system directory tree
        """
        # THIS METHOD MUST BE RUN UNDER DOMAIN STORAGE LOCK
        self.log.info("setting master domain for spUUID %s on sdUUID=%s",
                      self.spUUID, domain.sdUUID)
        futurePoolMD = self._getPoolMD(domain)
        with futurePoolMD.transaction():
            domain.changeLeaseParams(leaseParams)
            for spUUID in domain.getPools():
                if spUUID != self.spUUID:
                    self.log.warn("Force detaching from pool `%s` because of "
                                  "reconstruct master", spUUID)
                    domain.detach(spUUID)
            domain.attach(self.spUUID)
            domain.changeRole(sd.MASTER_DOMAIN)
            if not misc.isAscii(poolName) and not domain.supportsUnicode():
                raise se.UnicodeArgumentException()

            futurePoolMD.update({
                PMDK_SPM_ID: SPM_ID_FREE,
                PMDK_LVER: LVER_INVALID,
                PMDK_MASTER_VER: masterVersion,
                PMDK_POOL_DESCRIPTION: poolName,
                PMDK_DOMAINS: {domain.sdUUID: sd.DOM_ACTIVE_STATUS}})

    @unsecured
    def reconstructMaster(self, hostId, poolName, msdUUID, domDict,
                          masterVersion, leaseParams):
        self.log.info("spUUID=%s hostId=%s poolName=%s msdUUID=%s domDict=%s "
                      "masterVersion=%s leaseparams=(%s)", self.spUUID, hostId,
                      poolName, msdUUID, domDict, masterVersion, leaseParams)

        if msdUUID not in domDict:
            raise se.InvalidParameterException("masterDomain", msdUUID)

        futureMaster = sdCache.produce(msdUUID)

        # @deprecated, domain version < 3
        # For backward compatibility we must support a reconstructMaster
        # that doesn't specify an hostId.
        if not hostId:
            self._acquireTemporaryClusterLock(msdUUID, leaseParams)
            temporaryLock = True
        else:
            # Forcing to acquire the host id (if it's not acquired already).
            futureMaster.acquireHostId(hostId)
            futureMaster.acquireClusterLock(hostId)

            # The host id must be set for createMaster(...).
            self.id = hostId
            temporaryLock = False

        try:
            self.createMaster(poolName, futureMaster, masterVersion,
                              leaseParams)

            for sdUUID in domDict:
                domDict[sdUUID] = domDict[sdUUID].capitalize()

            # Add domain to domain list in pool metadata.
            self.log.info("Set storage pool domains: %s", domDict)
            self._getPoolMD(futureMaster).update({PMDK_DOMAINS: domDict})

            self.refresh(msdUUID=msdUUID, masterVersion=masterVersion)
        finally:
            if temporaryLock:
                self._releaseTemporaryClusterLock(msdUUID)
                self.stopMonitoringDomains()
            else:
                futureMaster.releaseClusterLock()

    @unsecured
    def copyPoolMD(self, prevMd, newMD):
        prevPoolMD = self._getPoolMD(prevMd)
        domains = prevPoolMD[PMDK_DOMAINS]
        pool_descr = prevPoolMD[PMDK_POOL_DESCRIPTION]
        lver = prevPoolMD[PMDK_LVER]
        spmId = prevPoolMD[PMDK_SPM_ID]
        # This is actually domain metadata, But I can't change this because of
        # backward compatibility
        leaseParams = prevMd.getLeaseParams()

        # Now insert pool metadata into new mastersd metadata

        newPoolMD = self._getPoolMD(newMD)
        with newPoolMD.transaction():
            newPoolMD.update({
                PMDK_DOMAINS: domains,
                PMDK_POOL_DESCRIPTION: pool_descr,
                PMDK_LVER: lver,
                PMDK_SPM_ID: spmId})
            newMD.changeLeaseParams(leaseParams)

    @unsecured
    def _copyLeaseParameters(self, srcDomain, dstDomain):
        leaseParams = srcDomain.getLeaseParams()
        self.log.info("Updating lease parameters for domain %s to %s",
                      srcDomain.sdUUID, leaseParams)
        dstDomain.changeLeaseParams(leaseParams)

    @unsecured
    def masterMigrate(self, sdUUID, msdUUID, masterVersion):
        self.log.info("sdUUID=%s spUUID=%s msdUUID=%s", sdUUID, self.spUUID,
                      msdUUID)

        # TODO: is this check still relevant?
        # Make sure the masterVersion higher than that of the pool
        if not masterVersion > self.getMasterVersion():
            raise se.StoragePoolWrongMaster(self.spUUID,
                                            self.masterDomain.sdUUID)

        curmsd = sdCache.produce(sdUUID)
        newmsd = sdCache.produce(msdUUID)
        self._refreshDomainLinks(newmsd)
        curmsd.invalidateMetadata()
        self._convertDomain(newmsd, curmsd.getFormat())
        self._copyLeaseParameters(curmsd, newmsd)

        # new 'master' should be in 'active' status
        domList = self.getDomains()
        if msdUUID not in domList:
            raise se.StorageDomainNotInPool(self.spUUID, msdUUID)
        if msdUUID == sdUUID:
            raise se.InvalidParameterException("msdUUID", msdUUID)
        if domList[msdUUID] != sd.DOM_ACTIVE_STATUS:
            raise se.StorageDomainNotActive(msdUUID)
        if newmsd.isISO():
            raise se.IsoCannotBeMasterDomain(msdUUID)
        if newmsd.isBackup():
            raise se.BackupCannotBeMasterDomain(msdUUID)

        # Copy master file system content to the new master
        src = os.path.join(curmsd.domaindir, sd.MASTER_FS_DIR)
        dst = os.path.join(newmsd.domaindir, sd.MASTER_FS_DIR)

        # Mount new master file system
        newmsd.mountMaster()
        # Make sure there is no cruft left over
        for dir in [newmsd.getVMsDir(), newmsd.getTasksDir()]:
            fileUtils.cleanupdir(dir)

        try:
            fileUtils.tarCopy(src, dst, exclude=("./lost+found",))
        except fileUtils.TarCopyFailed:
            self.log.error("tarCopy failed", exc_info=True)
            # Failed to copy the master data
            try:
                newmsd.unmountMaster()
            except Exception:
                self.log.error("Unexpected error", exc_info=True)
            raise se.StorageDomainMasterCopyError(msdUUID)

        self.copyPoolMD(curmsd, newmsd)

        path = newmsd.getMDPath()
        if not path:
            newmsd.unmountMaster()
            raise se.StorageDomainLayoutError("domain", msdUUID)

        # Acquire cluster lock on new master
        try:
            # If the new master domain is using safelease (version < 3) then
            # we can speed up the cluster lock acquirement by resetting the
            # SPM lease.
            # XXX: With SANLock there is no need to speed up the process (the
            # acquirement will take a short time anyway since we already hold
            # the host id) and more importantly resetting the lease is going
            # to interfere with the regular SANLock behavior.
            # @deprecated this is relevant only for domain version < 3
            if not newmsd.hasVolumeLeases():
                newmsd.initSPMlease()
            # Forcing to acquire the host id (if it's not acquired already)
            newmsd.acquireHostId(self.id)
            newmsd.acquireClusterLock(self.id)
        except Exception:
            self.log.error("Unexpected error", exc_info=True)
            newmsd.releaseClusterLock()
            newmsd.unmountMaster()
            raise
        self.log.debug("masterMigrate - lease acquired successfully")

        try:
            # Now mark new domain as 'master'
            # if things break down here move the master back pronto
            newPoolMD = self._getPoolMD(newmsd)
            with newPoolMD.transaction():
                newPoolMD[PMDK_MASTER_VER] = masterVersion
                newmsd.changeRole(sd.MASTER_DOMAIN)
        except Exception:
            self.log.error("Unexpected error", exc_info=True)
            newmsd.releaseClusterLock()
            newmsd.unmountMaster()
            raise

        # From this point on we have a new master and should not fail
        try:
            # Now recreate 'mastersd' link
            # we can use refresh() to do the job
            self.refresh(msdUUID, masterVersion)

            # From this point on there is a new master domain in the pool
            # Now that we are beyond the critical point we can clean up things
            curmsd.changeRole(sd.REGULAR_DOMAIN)

            # Clean up the old data from previous master fs
            for directory in [curmsd.getVMsDir(), curmsd.getTasksDir()]:
                fileUtils.cleanupdir(directory)

            # NOTE: here we unmount the *previous* master file system !!!
            curmsd.unmountMaster()
        except Exception:
            self.log.error("Unexpected error", exc_info=True)

        try:
            # Release old lease
            curmsd.releaseClusterLock()
        except Exception:
            self.log.error("Unexpected error", exc_info=True)

    def attachSD(self, sdUUID):
        """
        Attach a storage domain to the storage pool.
        This marks the storage domain as "attached" and links it
        to the storage pool
         'sdUUID' - storage domain UUID
        """
        self.log.info("sdUUID=%s spUUID=%s", sdUUID, self.spUUID)

        domains = self.getDomains()
        if sdUUID in domains:
            return True

        if len(domains) >= self.getMaximumSupportedDomains():
            raise se.TooManyDomainsInStoragePoolError()

        try:
            dom = sdCache.produce(sdUUID)
        except se.StorageDomainDoesNotExist:
            sdCache.invalidateStorage()
            dom = sdCache.produce(sdUUID)
        dom.acquireHostId(self.id)

        try:
            dom.acquireClusterLock(self.id)

            try:
                domVers = dom.getVersion()
                mstVers = self.masterDomain.getVersion()

                # If you remove this condition, remove it from
                # public_createStoragePool too.
                if dom.isData() and domVers > mstVers:
                    raise se.MixedSDVersionError(dom.sdUUID, domVers,
                                                 self.masterDomain.sdUUID,
                                                 mstVers)

                dom.attach(self.spUUID)
                domains[sdUUID] = sd.DOM_ATTACHED_STATUS
                self.setDomainsMap(domains)
                self._refreshDomainLinks(dom)

            finally:
                dom.releaseClusterLock()

        finally:
            dom.releaseHostId(self.id)

        self.updateMonitoringThreads()

    def forcedDetachSD(self, sdUUID):
        self.log.warn("Force detaching domain `%s`", sdUUID)
        domains = self.getDomains()

        if sdUUID not in domains:
            return True

        del domains[sdUUID]

        self.setDomainsMap(domains)
        self._cleanupDomainLinks(sdUUID)

        # If the domain that we are detaching is the master domain
        # we attempt to stop the SPM before releasing the host id
        if self.masterDomain.sdUUID == sdUUID:
            self.stopSpm()

        self.updateMonitoringThreads()
        self.log.debug("Force detach for domain `%s` is done", sdUUID)

    def detachSD(self, sdUUID):
        """
        Detach a storage domain from a storage pool.
        This removes the storage domain entry in the storage pool meta-data
        and leaves the storage domain in 'unattached' status.
         'sdUUID' - storage domain UUID
        """

        self.log.info("sdUUID=%s spUUID=%s", sdUUID, self.spUUID)

        dom = sdCache.produce(sdUUID)

        # Avoid detach domains if not owned by pool
        self.validateAttachedDomain(dom)

        if sdUUID == self.masterDomain.sdUUID:
            raise se.CannotDetachMasterStorageDomain(sdUUID)

        # TODO: clusterLock protection should be moved to
        #       StorageDomain.[at,de]tach()
        detachingISO = dom.isISO()

        if detachingISO:
            # An ISO domain can be shared by multiple pools
            dom.acquireHostId(self.id)
            dom.acquireClusterLock(self.id)

        try:
            # Remove pool info from domain metadata
            dom.detach(self.spUUID)
        finally:
            if detachingISO:
                dom.releaseClusterLock()
                dom.releaseHostId(self.id)

        # Remove domain from pool metadata
        self.forcedDetachSD(sdUUID)

    @unsecured
    def _convertDomain(self, domain, targetFormat=None):
        # Remember to get the sdUUID before upgrading because the object is
        # broken after the upgrade
        sdUUID = domain.sdUUID
        isMsd = (self.masterDomain.sdUUID == sdUUID)

        if targetFormat is None:
            targetFormat = self.getFormat()

        try:
            self._formatConverter.convert(
                self.poolPath, self.id, domain.getRealDomain(), isMsd,
                targetFormat)
        finally:
            # For safety we remove the domain from the cache also if the
            # conversion supposedly failed.
            sdCache.manuallyRemoveDomain(sdUUID)
            sdCache.produce(sdUUID)

    @unsecured
    def getFormat(self):
        return str(self.getVersion())

    def activateSD(self, sdUUID):
        """
        Activate a storage domain that is already a member in a storage pool.
        Validate that the storage domain is owned by the storage pool.
         'sdUUID' - storage domain UUID
        """
        self.log.info("sdUUID=%s spUUID=%s", sdUUID, self.spUUID)

        dom = sdCache.produce(sdUUID)
        # Avoid domain activation if not owned by pool
        self.validateAttachedDomain(dom)

        # Do nothing if already active
        domainStatuses = self.getDomains()
        if domainStatuses[sdUUID] == sd.DOM_ACTIVE_STATUS:
            return True

        # Domain conversion requires the links to be present
        self._refreshDomainLinks(dom)
        self.setDomainRegularRole(dom)

        if dom.getDomainClass() == sd.DATA_DOMAIN:
            self._convertDomain(dom)

        dom.activate()
        # set domains also do rebuild
        domainStatuses[sdUUID] = sd.DOM_ACTIVE_STATUS
        self.setDomainsMap(domainStatuses)
        self.updateMonitoringThreads()
        return True

    def deactivateSD(self, sdUUID, newMsdUUID, masterVersion):
        """
        Deactivate a storage domain.
        Validate that the storage domain is owned by the storage pool.
        Change storage domain status to "Attached" in the storage pool
        meta-data.

        :param sdUUID: The UUID of the storage domain you want to deactivate.
        :param newMsdUUID: The UUID of the new master storage domain.
        :param masterVersion: new master storage domain version
        """

        self.log.info("sdUUID=%s spUUID=%s newMsdUUID=%s", sdUUID, self.spUUID,
                      newMsdUUID)
        domList = self.getDomains()

        if sdUUID not in domList:
            raise se.StorageDomainNotInPool(self.spUUID, sdUUID)

        try:
            dom = sdCache.produce(sdUUID)
        except (se.StorageException, AttributeError, Timeout):
            # AttributeError: Unreloadable blockSD
            # Timeout: NFS unreachable domain
            self.log.warn("deactivating missing domain %s", sdUUID,
                          exc_info=True)
            if newMsdUUID != sd.BLANK_UUID:
                # Trying to migrate master failed to reach actual msd
                raise se.StorageDomainAccessError(sdUUID)

        else:
            if self.masterDomain.sdUUID == sdUUID:
                if newMsdUUID == sd.BLANK_UUID:
                    # TODO: For backward compatibility VDSM is silently
                    #       ignoring the deactivation request for the master
                    #       domain. Remove this check as soon as possible.
                    self.log.info("Silently ignoring the deactivation request "
                                  "for the master domain %s", sdUUID)
                    return
                else:
                    self.masterMigrate(sdUUID, newMsdUUID, masterVersion)
            else:
                masterDir = os.path.join(dom.domaindir, sd.MASTER_FS_DIR)
                try:
                    m = mount.getMountFromTarget(masterDir)
                except OSError as e:
                    if e.errno == errno.ENOENT:
                        pass  # Master is not mounted
                    else:
                        raise
                else:
                    try:
                        m.umount()
                    except mount.MountError:
                        self.log.error("Can't umount masterDir %s for domain "
                                       "%s", masterDir, dom)

        domList[sdUUID] = sd.DOM_ATTACHED_STATUS
        self.setDomainsMap(domList)
        self.updateMonitoringThreads()

    @unsecured
    def _linkStorageDomain(self, src, linkName):
        self.log.info("Linking %s to %s", src, linkName)
        try:
            current = os.readlink(linkName)
        except OSError as e:
            if e.errno != errno.ENOENT:
                self.log.error("Can't link SD %s to %s", src, linkName,
                               exc_info=True)
                return
        else:
            if current == linkName:
                return  # Nothing to do
        #Rebuild the link
        tmp_link_name = os.path.join(self.storage_repository,
                                     str(uuid.uuid4()))
        os.symlink(src, tmp_link_name)     # make tmp_link
        os.rename(tmp_link_name, linkName)

    @unsecured
    def _cleanupDomainLinks(self, domain):
        linkPath = os.path.join(self.poolPath, domain)
        try:
            os.remove(linkPath)
        except (OSError, IOError):
            pass

    @unsecured
    def _refreshDomainLinks(self, domain):
        domain.refreshDirTree()
        linkName = os.path.join(self.poolPath, domain.sdUUID)
        self._linkStorageDomain(domain.domaindir, linkName)
        if self.masterDomain.sdUUID == domain.sdUUID:
            masterName = os.path.join(self.poolPath, POOL_MASTER_DOMAIN)
            self._linkStorageDomain(domain.domaindir, masterName)

    @unsecured
    def __rebuild(self, msdUUID, masterVersion):
        """
        Rebuild storage pool.
        """
        # master domain must be refreshed first
        self.setMasterDomain(msdUUID, masterVersion)

        fileUtils.createdir(self.poolPath)

        # Find out all domains for future cleanup
        domainpat = os.path.join(self.poolPath, constants.UUID_GLOB_PATTERN)
        oldLinks = set(iglob(domainpat))

        # We should not rebuild non-active domains, because
        # they are probably disconnected from the host
        domUUIDs = self.getDomains(activeOnly=True).keys()

        #msdUUID should be present and active in getDomains result.
        #TODO: Consider remove if clause.
        if msdUUID in domUUIDs:
            domUUIDs.remove(msdUUID)

        #TODO: Consider to remove this whole block. UGLY!
        #We want to avoid looking up (vgs) of unknown block domains.
        #domUUIDs includes all the domains, file or block.
        block_mountpoint = os.path.join(sd.StorageDomain.storage_repository,
                                        sd.DOMAIN_MNT_POINT, sd.BLOCKSD_DIR)
        blockDomUUIDs = [vg.name for vg in blockSD.lvm.getVGs(domUUIDs)]
        domDirs = {}  # {domUUID: domaindir}
        #Add the block domains
        for domUUID in blockDomUUIDs:
            domaindir = os.path.join(block_mountpoint, domUUID)
            domDirs[domUUID] = domaindir
            # create domain special volumes folder
            fileUtils.createdir(os.path.join(domaindir, sd.DOMAIN_META_DATA))
            fileUtils.createdir(os.path.join(domaindir, sd.DOMAIN_IMAGES))
        #Add the file domains
        for domUUID, domaindir in fileSD.scanDomains():  \
                # [(fileDomUUID, file_domaindir)]
            if domUUID in domUUIDs:
                domDirs[domUUID] = domaindir

        #Link all the domains to the pool
        for domUUID, domaindir in domDirs.iteritems():
            linkName = os.path.join(self.poolPath, domUUID)
            self._linkStorageDomain(domaindir, linkName)
            oldLinks.discard(linkName)

        # Always try to build master links
        try:
            self._refreshDomainLinks(self.masterDomain)
        except (se.StorageException, OSError):
            self.log.error("_refreshDomainLinks failed for master domain %s",
                           self.masterDomain.sdUUID, exc_info=True)
        linkName = os.path.join(self.poolPath, self.masterDomain.sdUUID)
        oldLinks.discard(linkName)

        # Cleanup old trash from the pool
        for oldie in oldLinks:
            try:
                os.remove(oldie)
            except OSError as e:
                if e.errno != errno.ENOENT:
                    self.log.warn("Could not clean all trash from the pool dom"
                                  " `%s` (%s)", oldie, e)
            except Exception as e:
                    self.log.warn("Could not clean all trash from the pool dom"
                                  " `%s` (%s)", oldie, e)

    @unsecured
    def refresh(self, msdUUID, masterVersion):
        """
        Refresh storage pool.
         'msdUUID' - master storage domain UUID
        """
        sdCache.invalidateStorage()
        sdCache.refresh()
        self.__rebuild(msdUUID=msdUUID, masterVersion=masterVersion)

    def updateVM(self, vmList, sdUUID):
        """
        Update VMs.
         'vmList' - [{'vm':vmUUID,'ovf','imglist':'imgUUID1,imgUUID2,...'},...]
         'sdUUID' - target domain UUID, if not None, VM Images and the master
                    tree must be located on this domain.
                    If sdUUID is None, the update is on the pool, and therefore
                    the master domain will be updated.
        """
        self.log.info("spUUID=%s sdUUID=%s", self.spUUID, sdUUID)
        vms = self._getVMsPath(sdUUID)
        # We should exclude 'masterd' link from IMG_METAPATTERN globing
        vmUUID = ovf = imgList = ''
        for vm in vmList:
            if not vm:
                continue
            try:
                vmUUID = vm['vm']
                ovf = vm['ovf']
                imgList = vm['imglist'].split(',')
                self.log.info("vmUUID=%s imgList=%s", vmUUID, str(imgList))
            except KeyError:
                raise se.InvalidParameterException("vmList", str(vmList))

            vmPath = os.path.join(vms, vmUUID)
            if fileUtils.pathExists(vmPath):
                try:
                    fileUtils.cleanupdir(vmPath, ignoreErrors=False)
                except RuntimeError as e:
                    raise se.MiscDirCleanupFailure(str(e))

            try:
                fileUtils.createdir(vmPath)
                codecs.open(os.path.join(vmPath, vmUUID + '.ovf'), 'w',
                            encoding='utf8').write(ovf)
            except OSError as ex:
                if ex.errno == errno.ENOSPC:
                    raise se.NoSpaceLeftOnDomain(sdUUID)

                raise

    def removeVM(self, vmUUID, sdUUID):
        """
        Remove VM.
         'vmUUID' - Virtual machine UUID
        """
        self.log.info("spUUID=%s vmUUID=%s sdUUID=%s", self.spUUID, vmUUID,
                      sdUUID)
        vms = self._getVMsPath(sdUUID)
        if os.path.exists(os.path.join(vms, vmUUID)):
            fileUtils.cleanupdir(os.path.join(vms, vmUUID))

    def setDescription(self, descr):
        """
        Set storage pool description.
         'descr' - pool description
        """
        if len(descr) > MAX_POOL_DESCRIPTION_SIZE:
            raise se.StoragePoolDescriptionTooLongError()

        self.log.info("spUUID=%s descr=%s", self.spUUID, descr)

        if not misc.isAscii(descr) and not self.masterDomain.supportsUnicode():
            raise se.UnicodeArgumentException()

        self.setMetaParam(PMDK_POOL_DESCRIPTION, descr)

    def extendVolume(self, sdUUID, volumeUUID, size, isShuttingDown=None):
        # This method is not exposed through the remote API but it's called
        # directly from the mailbox to implement the thin provisioning on
        # block devices. The scope of this method is to extend only the
        # volume apparent size; the virtual disk size seen by the guest is
        # unchanged.
        sdCache.produce(sdUUID).extendVolume(volumeUUID, size, isShuttingDown)

    def extendVolumeSize(self, sdUUID, imgUUID, volUUID, newSize):
        imageResourcesNamespace = sd.getNamespace(sdUUID, IMAGE_NAMESPACE)
        with rmanager.acquireResource(imageResourcesNamespace, imgUUID,
                                      rm.LockType.exclusive):
            return sdCache.produce(sdUUID) \
                .produceVolume(imgUUID, volUUID).extendSize(int(newSize))

    @classmethod
    def _getPoolMD(cls, domain):
        # This might look disgusting but this makes it so that
        # This is the only intrusion needed to satisfy the
        # unholy union between pool and SD metadata
        return DictValidator(domain._metadata._dict, SP_MD_FIELDS)

    @property
    def _metadata(self):
        return self._getPoolMD(self.masterDomain)

    @unsecured
    def getDescription(self):
        try:
            return self.getMetaParam(PMDK_POOL_DESCRIPTION)
            # There was a bug that cause pool description to
            # disappear. Returning "" might be ugly but it keeps
            # everyone happy.
        except KeyError:
            return ""

    @unsecured
    def getVersion(self):
        return self.masterDomain.getVersion()

    @unsecured
    def getInfo(self):
        """
        Get storage pool info.
        """
        try:
            msdInfo = self.masterDomain.getInfo()
        except Exception:
            self.log.error("Couldn't read from master domain", exc_info=True)
            raise se.StoragePoolMasterNotFound(self.spUUID,
                                               self.masterDomain.sdUUID)

        try:
            pmd = self._getPoolMD(self.masterDomain)
        except Exception:
            self.log.error("Pool metadata error", exc_info=True)
            raise se.StoragePoolActionError(self.spUUID)

        poolInfo = {
            'type': msdInfo['type'],
            'name': pmd[PMDK_POOL_DESCRIPTION],
            'domains': domainListEncoder(pmd[PMDK_DOMAINS]),
            'master_uuid': self.masterDomain.sdUUID,
            'master_ver': pmd[PMDK_MASTER_VER],
            'lver': pmd[PMDK_LVER],
            'spm_id': pmd[PMDK_SPM_ID],
            'pool_status': 'uninitialized',
            'version': str(msdInfo['version']),
            'isoprefix': '',
            'pool_status': 'connected',
        }
        return poolInfo

    @unsecured
    def getIsoDomain(self):
        """
        Get pool's ISO domain if active
        """
        domDict = self.getDomains(activeOnly=True)
        for item in domDict:
            try:
                dom = sdCache.produce(item)
            except se.StorageDomainDoesNotExist:
                self.log.warn("Storage domain %s does not exist", item)
                continue

            if dom.isISO():
                return dom
        return None

    def setMetaParam(self, key, value):
        """
        Set key:value in pool metadata file
        """
        self._metadata[key] = value

    @unsecured
    def getMetaParam(self, key):
        """
        Get parameter from pool metadata file
        """
        return self._metadata[key]

    @unsecured
    def validateMasterDomainVersion(self, masterDomain, masterVersion):
        version = self._getPoolMD(masterDomain)[PMDK_MASTER_VER]
        if version != int(masterVersion):
            self.log.error("Requested master domain %s does not have expected "
                           "version %s it is version %s",
                           masterDomain.sdUUID, masterVersion, version)
            raise se.StoragePoolWrongMaster(self.spUUID, masterDomain.sdUUID)

    @unsecured
    def setMasterDomain(self, msdUUID, masterVersion):
        """
        Get the (verified) master domain of this pool.

        'msdUUID' - expected master domain UUID.
        'masterVersion' - expected pool msd version.
        """
        try:
            domain = sdCache.produce(msdUUID)
        except se.StorageDomainDoesNotExist:
            #Manager should start reconstructMaster if SPM.
            raise se.StoragePoolMasterNotFound(self.spUUID, msdUUID)

        if not domain.isMaster():
            self.log.error("Requested master domain %s is not a master domain "
                           "at all", msdUUID)
            raise se.StoragePoolWrongMaster(self.spUUID, msdUUID)

        pools = domain.getPools()
        if (self.spUUID not in pools):
            self.log.error("Requested master domain %s does not belong to pool"
                           " %s", msdUUID, self.spUUID)
            raise se.StoragePoolWrongMaster(self.spUUID, msdUUID)

        self.validateMasterDomainVersion(domain, masterVersion)
        self.log.debug("Master domain %s verified, version %s", msdUUID,
                       masterVersion)

        self.masterDomain = domain
        self.updateMonitoringThreads()

    @unsecured
    def invalidateMetadata(self):
        if not self.spmRole == SPM_ACQUIRED:
            self._metadata.invalidate()

    @unsecured
    @misc.samplingmethod
    def updateMonitoringThreads(self):
        # domain list it's list of sdUUID:status
        # sdUUID1:status1,sdUUID2:status2,...
        activeDomains = self.getDomains(activeOnly=True)
        monitoredDomains = self.domainMonitor.poolMonitoredDomains

        for sdUUID in monitoredDomains:
            if sdUUID not in activeDomains:
                try:
                    self.domainMonitor.stopMonitoring(sdUUID)
                    self.log.debug("Storage Pool `%s` stopped monitoring "
                                   "domain `%s`", self.spUUID, sdUUID)
                except se.StorageException:
                    self.log.error("Unexpected error while trying to stop "
                                   "monitoring domain `%s`", sdUUID,
                                   exc_info=True)

        for sdUUID in activeDomains:
            if sdUUID not in monitoredDomains:
                try:
                    self.domainMonitor.startMonitoring(sdUUID, self.id)
                    self.log.debug("Storage Pool `%s` started monitoring "
                                   "domain `%s`", self.spUUID, sdUUID)
                except se.StorageException:
                    self.log.error("Unexpected error while trying to monitor "
                                   "domain `%s`", sdUUID, exc_info=True)

    @unsecured
    def getDomains(self, activeOnly=False):
        return dict((sdUUID, status) for sdUUID, status
                    in self.getDomainsMap().iteritems()
                    if not activeOnly or status == sd.DOM_ACTIVE_STATUS)

    def checkBackupDomain(self):
        domDict = self.getDomains(activeOnly=True)
        for sdUUID in domDict:
            dom = sdCache.produce(sdUUID)
            if dom.isBackup():
                dom.mountMaster()
                # Master tree should be exist in this point
                # Recreate it if not.
                dom.createMasterTree()

    @unsecured
    def isActive(self, sdUUID):
        return sdUUID in self.getDomains(activeOnly=True)

    # TODO : move to sd.py
    @unsecured
    def _getVMsPath(self, sdUUID):
        """
        Return VMs dir within SD with sdUUID.
        """
        if not self.isActive(sdUUID):
            raise se.StorageDomainNotActive(sdUUID)
        vmPath = sdCache.produce(sdUUID).getVMsDir()
        # Get VMs path from the pool (from the master domain)

        if not os.path.exists(vmPath):
            raise se.VMPathNotExists(vmPath)
        return vmPath

    def copyImage(self, sdUUID, vmUUID, srcImgUUID, srcVolUUID, dstImgUUID,
                  dstVolUUID, descr, dstSdUUID, volType, volFormat,
                  preallocate, postZero, force):
        """
        Creates a new template/volume from VM.
        It does this it by collapse and copy the whole chain
        (baseVolUUID->srcVolUUID).

        :param sdUUID: The UUID of the storage domain in which the image
                       resides.
        :type sdUUID: UUID
        :param vmUUID: The UUID of the virtual machine you want to copy from.
        :type vmUUID: UUID
        :param srcImageUUID: The UUID of the source image you want to copy
                             from.
        :type srcImageUUID: UUID
        :param srcVolUUID: The UUID of the source volume you want to copy from.
        :type srcVolUUID: UUID
        :param dstImageUUID: The UUID of the destination image you want to copy
                             to.
        :type dstImageUUID: UUID
        :param dstVolUUID: The UUID of the destination volume you want to copy
                           to.
        :type dstVolUUID: UUID
        :param descr: The human readable description of the new template.
        :type descr: str
        :param dstSdUUID: The UUID of the destination storage domain you want
                          to copy to.
        :type dstSdUUID: UUID
        :param volType: The volume type of the volume being copied to.
        :type volType: some enum?!
        :param volFormat: The format of the volume being copied to.
        :type volFormat: some enum?!
        :param preallocate: Should the data be preallocated.
        :type preallocate: bool
        :param postZero: ?
        :type postZero: ?
        :param force: Should the copy be forced.
        :type force: bool

        :returns: a dict containing the UUID of the newly created image.
        :rtype: dict
        """
        srcImageResourcesNamespace = sd.getNamespace(sdUUID, IMAGE_NAMESPACE)
        if dstSdUUID not in (sdUUID, sd.BLANK_UUID):
            dstImageResourcesNamespace = sd.getNamespace(dstSdUUID,
                                                         IMAGE_NAMESPACE)
        else:
            dstImageResourcesNamespace = srcImageResourcesNamespace

        with nested(rmanager.acquireResource(srcImageResourcesNamespace,
                                             srcImgUUID, rm.LockType.shared),
                    rmanager.acquireResource(dstImageResourcesNamespace,
                                             dstImgUUID, rm.LockType.exclusive)
                    ):
            dstUUID = image.Image(self.poolPath).copyCollapsed(
                sdUUID, vmUUID, srcImgUUID, srcVolUUID, dstImgUUID,
                dstVolUUID, descr, dstSdUUID, volType, volFormat, preallocate,
                postZero, force)

        return dict(uuid=dstUUID)

    def moveImage(self, srcDomUUID, dstDomUUID, imgUUID, vmUUID, op, postZero,
                  force):
        """
        Moves or Copies an image between storage domains within the same
        storage pool.

        :param spUUID: The storage pool where the operation will take place.
        :type spUUID: UUID
        :param srcDomUUID: The UUID of the storage domain you want to copy
                           from.
        :type srcDomUUID: UUID
        :param dstDomUUID: The UUID of the storage domain you want to copy to.
        :type dstDomUUID: UUID
        :param imgUUID: The UUID of the image you want to copy.
        :type imgUUID: UUID
        :param vmUUID: The UUID of the vm that owns the images. ?
        :type vmUUID: UUID
        :param op: The operation code?
        :type op: some enum?
        :param postZero: ?
        :param force: Should the operation be forced.
        :type force: bool
        """
        srcImageResourcesNamespace = sd.getNamespace(srcDomUUID,
                                                     IMAGE_NAMESPACE)
        dstImageResourcesNamespace = sd.getNamespace(dstDomUUID,
                                                     IMAGE_NAMESPACE)
        # For MOVE_OP acquire exclusive lock
        # For COPY_OP shared lock is enough
        if op == image.MOVE_OP:
            srcLock = rm.LockType.exclusive
        elif op == image.COPY_OP:
            srcLock = rm.LockType.shared
        else:
            raise se.MoveImageError(imgUUID)

        with nested(rmanager.acquireResource(srcImageResourcesNamespace,
                                             imgUUID, srcLock),
                    rmanager.acquireResource(dstImageResourcesNamespace,
                                             imgUUID, rm.LockType.exclusive)):
            image.Image(self.poolPath).move(srcDomUUID, dstDomUUID, imgUUID,
                                            vmUUID, op, postZero, force)

    def cloneImageStructure(self, sdUUID, imgUUID, dstSdUUID):
        """
        Clone an image structure from a source domain to a destination domain
        within the same pool.

        :param spUUID: The storage pool where the operation will take place.
        :type spUUID: UUID
        :param sdUUID: The UUID of the storage domain you want to copy from.
        :type sdUUID: UUID
        :param imgUUID: The UUID of the image you want to copy.
        :type imgUUID: UUID
        :param dstSdUUID: The UUID of the storage domain you want to copy to.
        :type dstSdUUID: UUID
        """
        srcImgResNs = sd.getNamespace(sdUUID, IMAGE_NAMESPACE)
        dstImgResNs = sd.getNamespace(dstSdUUID, IMAGE_NAMESPACE)

        # Preparing the ordered resource list to be acquired
        resList = (rmanager.acquireResource(*x) for x in sorted((
            (srcImgResNs, imgUUID, rm.LockType.shared),
            (dstImgResNs, imgUUID, rm.LockType.exclusive),
        )))

        with nested(*resList):
            image.Image(self.poolPath).cloneStructure(
                sdUUID, imgUUID, dstSdUUID)

    def syncImageData(self, sdUUID, imgUUID, dstSdUUID, syncType):
        """
        Synchronize image data between storage domains within same pool.

        :param spUUID: The storage pool where the operation will take place.
        :type spUUID: UUID
        :param sdUUID: The UUID of the storage domain you want to copy from.
        :type sdUUID: UUID
        :param imgUUID: The UUID of the image you want to copy.
        :type imgUUID: UUID
        :param dstSdUUID: The UUID of the storage domain you want to copy to.
        :type dstSdUUID: UUID
        :param syncType: The type of sync to perform (all volumes, etc.).
        :type syncType: syncType enum
        """
        srcImgResNs = sd.getNamespace(sdUUID, IMAGE_NAMESPACE)
        dstImgResNs = sd.getNamespace(dstSdUUID, IMAGE_NAMESPACE)

        # Preparing the ordered resource list to be acquired
        resList = (rmanager.acquireResource(*x) for x in sorted((
            (srcImgResNs, imgUUID, rm.LockType.shared),
            (dstImgResNs, imgUUID, rm.LockType.exclusive),
        )))

        with nested(*resList):
            image.Image(self.poolPath).syncData(
                sdUUID, imgUUID, dstSdUUID, syncType)

    def uploadImage(self, methodArgs, sdUUID, imgUUID, volUUID=None):
        """
        Upload an image to a remote endpoint using the specified method and
        methodArgs.
        """
        imgResourceLock = rmanager.acquireResource(
            sd.getNamespace(sdUUID, IMAGE_NAMESPACE), imgUUID,
            rm.LockType.shared)

        with imgResourceLock:
            return image.Image(self.poolPath) \
                .upload(methodArgs, sdUUID, imgUUID, volUUID)

    def downloadImage(self, methodArgs, sdUUID, imgUUID, volUUID=None):
        """
        Download an image from a remote endpoint using the specified method
        and methodArgs.
        """
        imgResourceLock = rmanager.acquireResource(
            sd.getNamespace(sdUUID, IMAGE_NAMESPACE), imgUUID,
            rm.LockType.exclusive)

        with imgResourceLock:
            return image.Image(self.poolPath) \
                .download(methodArgs, sdUUID, imgUUID, volUUID)

    def moveMultipleImages(self, srcDomUUID, dstDomUUID, imgDict, vmUUID,
                           force):
        """
        Moves multiple images between storage domains within same storage pool.

        :param spUUID: The storage pool where the operation will take place.
        :type spUUID: UUID
        :param srcDomUUID: The UUID of the storage domain you want to copy
                           from.
        :type srcDomUUID: UUID
        :param dstDomUUID: The UUID of the storage domain you want to copy to.
        :type dstDomUUID: UUID
        :param imgDict: A dict of images in for form of ``{somthing:idunno}``
        :type imgDict: dict
        :param vmUUID: The UUID of the vm that owns the images.
        :type vmUUID: UUID
        :param force: Should the operation be forced.
        :type force: bool
        """
        srcImageResourcesNamespace = sd.getNamespace(srcDomUUID,
                                                     IMAGE_NAMESPACE)
        dstImageResourcesNamespace = sd.getNamespace(dstDomUUID,
                                                     IMAGE_NAMESPACE)

        imgList = imgDict.keys()
        imgList.sort()

        resourceList = []
        for imgUUID in imgList:
            resourceList.append(rmanager.acquireResource(
                srcImageResourcesNamespace, imgUUID, rm.LockType.exclusive))
            resourceList.append(rmanager.acquireResource(
                dstImageResourcesNamespace, imgUUID, rm.LockType.exclusive))

        with nested(*resourceList):
            image.Image(self.poolPath).multiMove(
                srcDomUUID, dstDomUUID, imgDict, vmUUID, force)

    def mergeSnapshots(self, sdUUID, vmUUID, imgUUID, ancestor, successor,
                       postZero):
        """
        Merges the source volume to the destination volume.

        :param sdUUID: The UUID of the storage domain that contains the images.
        :type sdUUID: UUID
        :param spUUID: The UUID of the storage pool that contains the images.
        :type spUUID: UUID
        :param imgUUID: The UUID of the new image you will be created after
                        the merge.?
        :type imgUUID: UUID
        :param ancestor: The UUID of the source volume.?
        :type ancestor: UUID
        :param successor: The UUID of the destination volume.?
        :type successor: UUID
        :param postZero: ?
        :type postZero: bool?
        """
        imageResourcesNamespace = sd.getNamespace(sdUUID, IMAGE_NAMESPACE)

        with rmanager.acquireResource(imageResourcesNamespace, imgUUID,
                                      rm.LockType.exclusive):
            image.Image(self.poolPath).merge(
                sdUUID, vmUUID, imgUUID, ancestor, successor, postZero)

    def createVolume(self, sdUUID, imgUUID, size, volFormat, preallocate,
                     diskType, volUUID=None, desc="",
                     srcImgUUID=volume.BLANK_UUID,
                     srcVolUUID=volume.BLANK_UUID):
        """
        Creates a new volume.

        .. note::
            If the *imgUUID* is **identical** to the *srcImgUUID* the new
            volume will be logically considered a snapshot of the old volume.
            If the *imgUUID* is **different** from the *srcImgUUID* the old
            volume will be logically considered as a template of the new
            volume.

        :param sdUUID: The UUID of the storage domain that contains the volume.
        :type sdUUID: UUID
        :param imgUUID: The UUID of the image that the new volume will have.
        :type imgUUID: UUID
        :param size: The size of the new volume in bytes.
        :type size: int
        :param volFormat: The format of the new volume.
        :type volFormat: some enum ?!
        :param preallocate: Should the volume be preallocated.
        :type preallocate: bool
        :param diskType: The disk type of the new volume.
        :type diskType: :class:`API.Image.DiskTypes`
        :param volUUID: The UUID of the new volume that will be created.
        :type volUUID: UUID
        :param desc: A human readable description of the new volume.
        :param srcImgUUID: The UUID of the image that resides on the volume
                           that will be the base of the new volume.
        :type srcImgUUID: UUID
        :param srcVolUUID: The UUID of the volume that will be the base of the
                           new volume.
        :type srcVolUUID: UUID

        :returns: a dict with the UUID of the new volume.
        :rtype: dict
        """
        imageResourcesNamespace = sd.getNamespace(sdUUID, IMAGE_NAMESPACE)

        with rmanager.acquireResource(imageResourcesNamespace, imgUUID,
                                      rm.LockType.exclusive):
            newVolUUID = sdCache.produce(sdUUID).createVolume(
                imgUUID=imgUUID, size=size, volFormat=volFormat,
                preallocate=preallocate, diskType=diskType, volUUID=volUUID,
                desc=desc, srcImgUUID=srcImgUUID, srcVolUUID=srcVolUUID)
        return dict(uuid=newVolUUID)

    def deleteVolume(self, sdUUID, imgUUID, volumes, postZero, force):
        """
        Deletes a given volume.

        .. note::
            This function assumes:

                * If more than 1 volume, all volumes are a part of the **same**
                  chain.
                * Given volumes are ordered, so predecessor is deleted before
                  ancestor. ? (might be confused?)

        :param sdUUID: The UUID of the storage domain that contains the volume.
        :type sdUUID: UUID
        :param imgUUID: The UUID of the image that id that the new volume will
                        have.
        :type imgUUID: UUID
        """
        imageResourcesNamespace = sd.getNamespace(sdUUID, IMAGE_NAMESPACE)

        with rmanager.acquireResource(imageResourcesNamespace, imgUUID,
                                      rm.LockType.exclusive):
            dom = sdCache.produce(sdUUID)
            for volUUID in volumes:
                dom.produceVolume(imgUUID, volUUID).delete(
                    postZero=postZero, force=force)

    def deleteImage(self, domain, imgUUID, volsByImg):
        """
        Deletes a given list of volumes belonging to imgUUID.

        .. note::
            This function cannot be scheduled as it takes the domain object
            (for performance reasons) instead of the sdUUID.

            Few arguments could be evetually optimzed out and normalized but
            it requires some refactoring.

        :param domain: The object of the domain containing the image.
        :type sdUUID: StorageDomain
        :param imgUUID: The UUID of the relevant image.
        :type imgUUID: UUID
        :param volsByImg: List of the volumes to remove.
        :type volsByImg: list
        """
        domain.deleteImage(domain.sdUUID, imgUUID, volsByImg)

    def setMaxHostID(self, spUUID, maxID):
        """
        Set maximum host ID
        """
        self.log.error("TODO: Implement")
        self._maxHostID
        self.spmMailer.setMaxHostID(maxID)
        raise se.NotImplementedException

    def detachAllDomains(self):
        """
        Detach all domains from pool before destroying pool

        Assumed cluster lock and that SPM is already stopped.
        """
        # Find regular (i.e. not master) domains from the pool metadata
        regularDoms = tuple(sdUUID for sdUUID in self.getDomains()
                            if sdUUID != self.masterDomain.sdUUID)
        # The Master domain should be detached last
        for sdUUID in regularDoms:
            self.detachSD(sdUUID)

        # Forced detach master domain
        self.forcedDetachSD(self.masterDomain.sdUUID)
        self.masterDomain.detach(self.spUUID)

    def setVolumeDescription(self, sdUUID, imgUUID, volUUID, description):
        imageResourcesNamespace = sd.getNamespace(sdUUID, IMAGE_NAMESPACE)
        with rmanager.acquireResource(imageResourcesNamespace, imgUUID,
                                      rm.LockType.exclusive):
            sdCache.produce(sdUUID).produceVolume(
                imgUUID=imgUUID,
                volUUID=volUUID).setDescription(descr=description)

    def setVolumeLegality(self, sdUUID, imgUUID, volUUID, legality):
        imageResourcesNamespace = sd.getNamespace(sdUUID, IMAGE_NAMESPACE)
        with rmanager.acquireResource(imageResourcesNamespace, imgUUID,
                                      rm.LockType.exclusive):
            sdCache.produce(sdUUID).produceVolume(
                imgUUID=imgUUID,
                volUUID=volUUID).setLegality(legality=legality)

    def getVmsList(self, sdUUID=None):
        if sdUUID is None:
            dom = self.masterDomain
        else:
            dom = sdCache.produce(sdUUID)

        return dom.getVMsList()

    def getVmsInfo(self, sdUUID, vmList=None):
        return sdCache.produce(sdUUID).getVMsInfo(vmList=vmList)

    def validateVolumeChain(self, sdUUID, imgUUID):
        image.Image(self.poolPath).validateVolumeChain(sdUUID, imgUUID)

    def extendSD(self, sdUUID, devlist, force):
        sdCache.produce(sdUUID).extend(devlist, force)

    def setSDDescription(self, sd, description):
        sd.setDescription(descr=description)

    def getAllTasksStatuses(self):
        return self.taskMng.getAllTasksStatuses("spm")

    def getAllTasksInfo(self):
        return self.taskMng.getAllTasksInfo("spm")
