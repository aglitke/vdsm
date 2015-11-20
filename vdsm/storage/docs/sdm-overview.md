# SDM Overview
_Author: Adam Litke <alitke@redhat.com>_

This document aims to provide an overview of how vdsm manages shared
storage.

## Managing shared storage
oVirt makes use of a shared storage architecture primarily to store
virtual machine templates and disks.  Many hosts can be reading and
writing to the storage at the same time.  In order for the system to 1)
prevent corruption, 2) track status, and 3) recover from failures a
storage management architecture must be applied to the storage.  oVirt
has two such architectures: SPM (the original architecture), and SDM
(the next-generation architecture).  Both of these, described in more
detail below, address all three requirements.

All I/O to storage can be split into two types of operations: datapath
operations and metadata operations.  A datapath operation is I/O to or
from the data area of an existing disk.  Corruption of datapath
operations can be prevented by ensuring that only one host can have a
disk open for writing at any given time.  Metadata operations affect the
structure of data on storage.  For example, creating and deleting
volumes, changing storage domain or volume metadata, or attaching and
detaching storage domains.  Preventing corruption from these operations
is possible if they are performed by a single designated host and the
other hosts are properly notified of changes as appropriate.

It is important to know the status of all jobs that may be affecting the
storage.  Usually the system needs to wait for one step to complete
before advancing to the next step.  For example, when cloning a VM from
a template the VM should not be started until all data from the template
disk(s) has been synchronized to the VM's disk(s).  Therefore, the
storage management architecture must be able to report if a job is
running and, if finished, whether it succeeded or failed and for what
reason.  Ideally, the progress of long running jobs should also be
reported.

Any shared storage environment will experience faults.  Disks will fail,
networks will go down, hosts will shut down in the middle of operations,
software has bugs.  A storage management architecture should recover
from errors gracefully and be returned to a coherent state.  One way to
do this is by pushing recovery steps (rollbacks) onto a stack located on
the persistent storage.  If an operation fails, its recovery steps are
popped from the stack and executed in LIFO order to restore the system
to its previous state.  Another approach is to use atomic operations and
garbage collection.  All storage commands should be atomic if possible.
Then there can be no intermediate invalid state.  For complex commands
such as creating a new volume where multiple changes need to be made the
first change should create "garbage" on the storage and the last change
should atomically convert the "garbage" to something permanent.

## SPM storage management
SPM stands for Storage Pool Manager.  With this approach all storage
domains are organized under a storage pool.  Only one storage pool can
be active (connected) at a time on a host.  Exactly one storage domain
per pool is designated the master storage domain.  A special storage
area is allocated on the master storage domain to persist information
about storage tasks for the entire pool.  Each task includes the current
state (pending, running, failed, etc) and rollback steps (if required).

In order to prevent corruption, only one host at a time may make changes
to storage object metadata (storage domains, images, volume
information).  SAN-based locking is used to ensure that all hosts agree
on which host is granted this authority (called the SPM role).  If the
SPM host becomes unresponsive the SPM role is revoked and the host is
fenced to halt further modifications to the storage from that host.
Once fencing is complete it is safe to elect a new SPM host.  Upon
election to the SPM role a host will recover the state of tasks from the
master storage domain and perform any necessary rollback steps.  This
hosts exports APIs to fetch, abort, and clear the Tasks.  The selection
of the SPM host is handled by the centralized ovirt-engine manager which
makes a special API call to instruct a host to contend for the SPM role.

Datapath operations are secured by the ovirt-engine manager.  It ensures
that a VM cannot be started on multiple hosts and will not send datapath
commands to the SPM host if the VM is running.

## SDM Storage Management
SDM stands for Storage Domain Manager.  Storage domains are no longer
organized under a storage pool and there is no master storage domain.
Instead of the SPM role, any host may modify storage object metadata by
first acquiring the SAN-based metadata lock associated with the storage
domain to be altered.  A single host may hold metadata locks for any
number of storage domains.  SDM metadata locks are non-blocking.  If
another host holds the lock then any attempt to acquire it from a
different host will fail immediately.  Similar to SPM, a host holding at
least one metadata lock may be fenced if it becomes unresponsive.  The
metadata locks are acquired automatically by the host as required to
satisfy requests that are sent from ovirt-engine.  Per-domain metadata
locks are more scalable than a pool-wide lock because metadata
operations can be spread across multiple hosts.  This also avoids the
complexity of electing an SPM and managing the role.  One downside is
that ovirt-engine will need to handle commands that fail due to
contention on metadata locks.

The progress of storage jobs is monitored by polling the host on which
the operation was started.  Success or failure is determined by polling
the affected entity (eg. check if a volume exists).  Errors are reported
as vdsm events.  Interrupted operations leave garbage behind which can
be garbage collected.

### Storage Operations Monitoring
Regardless of expected running time, all SDM verbs create a storage job
which is used for tracking the status and progress of that verb.  All
SDM verbs require a job UUID to be passed as a parameter.  This UUID
identifies this verb in both the host jobs API and in emitted events.

The host jobs API (Host.getJobs) is a polling interface for checking the
status and progress of active jobs on a single host.  Only active jobs
are reported and once a job finishes (regardless of outcome) it will no
longer be reported.  This interface can be polled to update progress
while waiting for a verb to finish.

SDM verbs also trigger events using the supplied job UUID.  When the
verb finishes either a success or fail event will be emitted.  In case
of failure, the event includes additional information about the error
encountered.  Periodically progress events are emitted for long running
jobs.

If an event is not received and the job is not being reported then the
job has finished.  In this case entity polling must be used to determine
whether the verb succeeded or failed.  Each verb documents its success
and failure conditions.  For example, when creating a volume, success is
indicated if Volume.getInfo is successful when called with the new
volume's image and volume ID's.

### Garbage Collection
The SDM garbage collector finds evidence of partially completed storage
operations (garbage) on storage and reverts those changes to return the
storage to a consistent state.  All verbs have been carefully designed
to either complete atomically or leave a marker for the garbage
collector.

Storage domains are periodically garbage collected to maintain
consistency.  ovirt-engine selects a host and calls a vdsm verb to start
the process.  Garbage collection requires the domain metadata lock.
First, removed images are found and deleted.  Candidate volumes are
identified, verified, and deleted.

In addition to the batch mode described above, the garbage collector may
be invoked on a volume directly.  This in-line mode is used by the
removeVolume verb to immediately clean the image after a leaf volume is
removed from it.

There are currently two types of garbage: removed images, and volume
artifacts.  These are identified differently depending on the storage
domain type (block or file).  On file domains garbage images are
directories with a special extension and volume artifacts are identified
by a volume metadata file with a special extension.  Block domains can
only have garbage volumes which are identified by the presence of a
special LV tag.

Garbage collection is more effective than rollbacks for two reasons: it
does not rely on a separate storage area for rollback steps, and it it
resilient to failures.  As the garbage collector works, it only removes
the garbage marker when the garbage is completely cleaned up.  This way,
the process can be interrupted and restarted without problems.  If the
storage connection is unstable the garbage can be cleaned once stability
is restored.

### Verbs
Each SDM verb performs a single logical operation with evidence of
success or failure easily detectable on storage.  Complex operations can
be achieved by orchestrating a set of these simple verbs.

