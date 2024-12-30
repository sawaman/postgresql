/*-------------------------------------------------------------------------
 * logicalxlog.c
 *	   This module contains the codes for enabling or disabling to include
 *	   logical information into WAL records and logical decoding.
 *
 * Copyright (c) 2012-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/logicalxlog.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/transam.h"
#include "access/xloginsert.h"
#include "access/xlogrecovery.h"
#include "catalog/pg_control.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "replication/logicalxlog.h"
#include "replication/slot.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "utils/builtins.h"
#include "utils/wait_event.h"

typedef struct XLogLogicalInfoCtlData
{
	LogicalDecodingStatus status;
	slock_t		mutex;
	ConditionVariable cv;
}			XLogLogicalInfoCtlData;
XLogLogicalInfoCtlData *XLogLogicalInfoCtl = NULL;

static void logical_decoding_activation_abort_callback(int code, Datum arg);
static void do_activate_logical_decoding(void);

/*
 * Initialization of shared memory.
 */

Size
LogicalXlogShmemSize(void)
{
	return sizeof(XLogLogicalInfoCtlData);
}

void
LogicalXlogShmemInit(void)
{
	bool		found;

	XLogLogicalInfoCtl = ShmemInitStruct("Logical Info Logging",
										 sizeof(XLogLogicalInfoCtlData),
										 &found);

	if (!found)
	{
		XLogLogicalInfoCtl->status = LOGICAL_DECODING_STATUS_DISABLED;
		SpinLockInit(&XLogLogicalInfoCtl->mutex);
		ConditionVariableInit(&XLogLogicalInfoCtl->cv);
	}
}

/*
 * This must be called ONCE during postmaster or standalone-backend startup,
 * before calling StartupReplicationSlots().
 */
void
StartupLogicalDecodingStatus(bool enabled_at_last_checkpoint)
{
	LogicalDecodingStatus status;

	elog(LOG, "XXX logical dec was enabled? %d, wal_level %d",
		 enabled_at_last_checkpoint,
		 wal_level);

	if (enabled_at_last_checkpoint)
		status = LOGICAL_DECODING_STATUS_READY;
	else
		status = LOGICAL_DECODING_STATUS_DISABLED;

	/*
	 * On standbys, we always start with the status in the last checkpoint
	 * record. If changes of wal_level or logical decoding status is sent from
	 * the primary, we will enable or disable the logical decoding while
	 * replaying the WAL record and invalidate slots if necessary.
	 */
	if (!StandbyMode)
	{
		/*
		 * Disable logical decoding if replication slots are not available.
		 */
		if (max_replication_slots == 0)
			status = LOGICAL_DECODING_STATUS_DISABLED;

		/*
		 * If previously the logical decoding was active but the server
		 * restarted with wal_level < 'replica', we disable the logical
		 * decoding.
		 */
		if (wal_level < WAL_LEVEL_REPLICA)
			status = LOGICAL_DECODING_STATUS_DISABLED;

		/*
		 * Setting wal_level to 'logical' immediately enables logical decoding
		 * and WAL-logging logical info.
		 */
		if (wal_level >= WAL_LEVEL_LOGICAL)
			status = LOGICAL_DECODING_STATUS_READY;
	}

	XLogLogicalInfoCtl->status = status;
}

void
UpdateLogicalDecodingStatus(bool activate)
{
	XLogLogicalInfoCtl->status = activate
		? LOGICAL_DECODING_STATUS_READY
		: LOGICAL_DECODING_STATUS_DISABLED;
}

/*
 * Is WAL-Logging logical info enabled?
 */
bool inline
XLogLogicalInfoEnabled(void)
{
	/*
	 * Use volatile pointer to make sure we make a fresh read of the shared
	 * variable.
	 */
	volatile	XLogLogicalInfoCtlData *ctl = XLogLogicalInfoCtl;

	return ctl->status >= LOGICAL_DECODING_STATUS_XLOG_LOGICALINFO;
}

/*
 * Is logical decoding active?
 */
bool inline
IsLogicalDecodingActive(void)
{
	/*
	 * Use volatile pointer to make sure we make a fresh read of the shared
	 * variable.
	 */
	volatile	XLogLogicalInfoCtlData *ctl = XLogLogicalInfoCtl;

	return ctl->status == LOGICAL_DECODING_STATUS_READY;
}

static void
do_activate_logical_decoding(void)
{
	bool		recoveryInProgress = RecoveryInProgress();

	/*
	 * Get the latest status of logical info logging. If it's already
	 * activated, quick return.
	 */
	SpinLockAcquire(&XLogLogicalInfoCtl->mutex);
	if (XLogLogicalInfoCtl->status >= LOGICAL_DECODING_STATUS_XLOG_LOGICALINFO)
	{
		SpinLockRelease(&XLogLogicalInfoCtl->mutex);
		return;
	}

	/* Activate the logical info logging */
	XLogLogicalInfoCtl->status = LOGICAL_DECODING_STATUS_XLOG_LOGICALINFO;
	SpinLockRelease(&XLogLogicalInfoCtl->mutex);

	PG_ENSURE_ERROR_CLEANUP(logical_decoding_activation_abort_callback, 0);
	{
		RunningTransactions running_xacts;

		running_xacts = GetRunningTransactionData();

		/*
		 * GetRunningTransactionData() acquired both ProcArrayLock and
		 * XidGenLock, we must release them.
		 */
		LWLockRelease(ProcArrayLock);
		LWLockRelease(XidGenLock);

		for (int i = 0; i < running_xacts->xcnt; i++)
		{
			TransactionId xid = running_xacts->xids[i];

			/*
			 * Upper layers should prevent that we ever need to wait on
			 * ourselves. Check anyway, since failing to do so would either
			 * result in an endless wait or an Assert() failure.
			 */
			if (TransactionIdIsCurrentTransactionId(xid))
				elog(ERROR, "waiting for ourselves");

			XactLockTableWait(xid, NULL, NULL, XLTW_None);
		}
	}
	PG_END_ENSURE_ERROR_CLEANUP(logical_decoding_activation_abort_callback, 0);

	START_CRIT_SECTION();

	/* Activate logical decoding on the database cluster */
	SpinLockAcquire(&XLogLogicalInfoCtl->mutex);
	XLogLogicalInfoCtl->status = LOGICAL_DECODING_STATUS_READY;
	SpinLockRelease(&XLogLogicalInfoCtl->mutex);

	if (XLogStandbyInfoActive() && !recoveryInProgress)
	{
		bool		enabled = true;

		XLogBeginInsert();
		XLogRegisterData((char *) (&enabled), sizeof(bool));

		XLogInsert(RM_XLOG_ID, XLOG_LOGICAL_DECODING_STATUS);
	}

	END_CRIT_SECTION();

	/* Let everybody know we've activated the logical decoding */
	ConditionVariableBroadcast(&XLogLogicalInfoCtl->cv);
}

static void
logical_decoding_activation_abort_callback(int code, Datum arg)
{
	SpinLockAcquire(&XLogLogicalInfoCtl->mutex);
	XLogLogicalInfoCtl->status = LOGICAL_DECODING_STATUS_DISABLED;
	SpinLockRelease(&XLogLogicalInfoCtl->mutex);

	/* Let everybody know we failed to activate the logical decoding */
	ConditionVariableBroadcast(&XLogLogicalInfoCtl->cv);
}

Datum
pg_activate_logical_decoding(PG_FUNCTION_ARGS)
{
	LogicalDecodingStatus status;

	if (max_replication_slots == 0)
		ereport(LOG,
				(errmsg("logical decoding can only be activated if \"max_replication_slots\" > 0")));

	if (wal_level < WAL_LEVEL_REPLICA)
		ereport(LOG,
				(errmsg("logical decoding can only be activated if \"wal_level\" >= \"replica\"")));

	if (IsTransactionState() &&
		GetTopTransactionIdIfAny() != InvalidTransactionId)
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				 errmsg("cannot activate logical decoding in transaction that has performed writes")));


	SpinLockAcquire(&(XLogLogicalInfoCtl->mutex));
	status = XLogLogicalInfoCtl->status;
	SpinLockRelease(&(XLogLogicalInfoCtl->mutex));

	if (status == LOGICAL_DECODING_STATUS_READY)
		PG_RETURN_VOID();

	/*
	 * Get ready to sleep until the logical decoding gets activated. We may
	 * end up not sleeping, but we don't want to do this while holding the
	 * spinlock.
	 */
	ConditionVariablePrepareToSleep(&XLogLogicalInfoCtl->cv);

	for (;;)
	{
		SpinLockAcquire(&XLogLogicalInfoCtl->mutex);
		status = XLogLogicalInfoCtl->status;
		SpinLockRelease(&XLogLogicalInfoCtl->mutex);

		/* Return if the logical decoding is activated */
		if (status == LOGICAL_DECODING_STATUS_READY)
			break;

		if (status == LOGICAL_DECODING_STATUS_XLOG_LOGICALINFO)
		{
			/*
			 * Someone has already started the activation process. Wait for
			 * the activation to complete and check the status again.
			 */
			ConditionVariableSleep(&XLogLogicalInfoCtl->cv,
								   WAIT_EVENT_LOGICAL_DECODING_ACTIVATION);

			continue;
		}

		/* Activate logical decoding */
		do_activate_logical_decoding();
	}

	ConditionVariableCancelSleep();

	PG_RETURN_VOID();
}

Datum
pg_deactivate_logical_decoding(PG_FUNCTION_ARGS)
{
	int			valid_logical_slots = 0;
	bool		recoveryInProgress = RecoveryInProgress();

	/*
	 * Hold ReplicationSlotAllocationLock to prevent slots from newly
	 * being created while deactivating the logical decoding.
	 */
	LWLockAcquire(ReplicationSlotAllocationLock, LW_SHARED);

	for (int i = 0; i < max_replication_slots; i++)
	{
		ReplicationSlot *s = &ReplicationSlotCtl->replication_slots[i];

		if (!s->in_use)
			continue;

		if (SlotIsPhysical(s))
			continue;

		if (s->data.invalidated != RS_INVAL_NONE)
			continue;

		valid_logical_slots++;
	}

	if (valid_logical_slots > 0)
		ereport(ERROR,
				(errmsg("cannot deactivate logical decoding while having valid logical replication slots")));

	if (XLogStandbyInfoActive() && !recoveryInProgress)
	{
		bool		enabled = false;

		XLogBeginInsert();
		XLogRegisterData((char *) (&enabled), sizeof(bool));

		XLogInsert(RM_XLOG_ID, XLOG_LOGICAL_DECODING_STATUS);
	}

	SpinLockAcquire(&(XLogLogicalInfoCtl->mutex));
	XLogLogicalInfoCtl->status = LOGICAL_DECODING_STATUS_DISABLED;
	SpinLockRelease(&(XLogLogicalInfoCtl->mutex));

	LWLockRelease(ReplicationSlotAllocationLock);

	PG_RETURN_BOOL(true);
}

Datum
pg_get_logical_decoding_status(PG_FUNCTION_ARGS)
{
	LogicalDecodingStatus status;
	const char *status_str = "unknown";

	SpinLockAcquire(&(XLogLogicalInfoCtl->mutex));
	status = XLogLogicalInfoCtl->status;
	SpinLockRelease(&(XLogLogicalInfoCtl->mutex));

	switch (status)
	{
		case LOGICAL_DECODING_STATUS_DISABLED:
			status_str = "disabled";
			break;
		case LOGICAL_DECODING_STATUS_XLOG_LOGICALINFO:
			status_str = "xlog-logicalinfo";
			break;
		case LOGICAL_DECODING_STATUS_READY:
			status_str = "ready";
			break;
	}

	PG_RETURN_TEXT_P(cstring_to_text(status_str));
}
