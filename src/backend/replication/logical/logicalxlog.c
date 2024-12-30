/*-------------------------------------------------------------------------
 * logicalxlog.c
 *	   This module contains the codes for enabling or disabling to include
 *	   logical information into WAL records.
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
	LogicalDecodingState state;
	slock_t		mutex;

	ConditionVariable		cv;
} XLogLogicalInfoCtlData;

/*
 *
 */
XLogLogicalInfoCtlData *XLogLogicalInfoCtl = NULL;

static void logical_decoding_activation_abort_callback(int code, Datum arg);

Size
LogicalXlogShmemSize(void)
{
	return sizeof(XLogLogicalInfoCtlData);
}

void
LogicalXlogShmemInit(void)
{
	bool	found;

	XLogLogicalInfoCtl = ShmemInitStruct("Logical Info Logging",
										 sizeof(XLogLogicalInfoCtlData),
										 &found);

	if (!found)
	{
		XLogLogicalInfoCtl->state = LOGICAL_DECODING_STATE_DISABLED;
		SpinLockInit(&XLogLogicalInfoCtl->mutex);
		ConditionVariableInit(&XLogLogicalInfoCtl->cv);
	}
}

/*
 * This must be called ONCE during postmaster or standalone-backend startup,
 * before calling StartupReplicationSlots().
 */
void
StartupLogicalDecodingState(bool enabled_at_last_checkpoint)
{
	LogicalDecodingState state;

	if (enabled_at_last_checkpoint)
		state = LOGICAL_DECODING_STATE_READY;
	else
		state = LOGICAL_DECODING_STATE_DISABLED;

	/*
	 * On standbys, we always start with the state in the last checkpoint
	 * record. If changes of wal_level or logical decoding state is sent
	 * from the primary, we will enable or disable the logical decoding
	 * while replaying the WAL record and invalidate slots if necessary.
	 */
	if (!StandbyMode)
	{
		/*
		 * Disable logical decoding if replication slots are not available.
		 * If there are replication slots serialized on the disk, we will
		 * error out when restoring them.
		 */
		if (max_replication_slots == 0)
			state = LOGICAL_DECODING_STATE_DISABLED;

		/*
		 * If previously the logical decoding was active but the server
		 * restarted with wal_level < 'replica', we disable the logical
		 * decoding. If there are replication slots serialized on the disk
		 * we will error out when restoring them.
		 */
		if (wal_level < WAL_LEVEL_REPLICA)
			state = LOGICAL_DECODING_STATE_DISABLED;

		/*
		 * Setting wal_level to 'logical' immediately enables logical
		 * decoding and WAL-logging logical info.
		 */
		if (wal_level >= WAL_LEVEL_LOGICAL)
		{
			elog(LOG, "XXX immdeitaly enable wal_level >= LOGICAL");
			state = LOGICAL_DECODING_STATE_READY;
		}
	}

	XLogLogicalInfoCtl->state = state;

	/*
	if (state == LOGICAL_DECODING_STATE_READY)
	{
		uint64 generation;

		generation = EmitProcSignalBarrier(PROCSIGNAL_BARRIER_UPDATELOGICALINFO);

		if (IsUnderPostmaster)
			WaitForProcSignalBarrier(generation);
	}
	*/
}

/*
 */
void
UpdateLogicalDecodingState(bool activate)
{
	XLogLogicalInfoCtl->state = activate
		? LOGICAL_DECODING_STATE_READY
		: LOGICAL_DECODING_STATE_DISABLED;

	EmitProcSignalBarrier(PROCSIGNAL_BARRIER_UPDATELOGICALINFO);
}

bool
ProcessBarrierUpdateLogicalInfo(void)
{
	//XlogLogicalInfoUpdateLocal();
	//elog(LOG, "XXX absorb global barrier now enabled? %d",
	//XLogLogicalInfo);

	return true;
}

bool inline
IsXLogLogicalInfoActive(void)
{
	/*
	 * Use volatile pointer to make sure we make a fresh read of
	 * the shared variable.
	 */
	volatile XLogLogicalInfoCtlData *ctl = XLogLogicalInfoCtl;

	return ctl->state >= LOGICAL_DECODING_STATE_XLOG_LOGICALINFO;
}

bool inline
IsLogicalDecodingActive(void)
{
	/*
	 * Use volatile pointer to make sure we make a fresh read of
	 * the shared variable.
	 */
	volatile XLogLogicalInfoCtlData *ctl = XLogLogicalInfoCtl;

	return ctl->state == LOGICAL_DECODING_STATE_READY;
}

void
EnsureLogicalDecodingActive(void)
{
	LogicalDecodingState state;

	if (max_replication_slots == 0)
		return;

	SpinLockAcquire(&(XLogLogicalInfoCtl->mutex));
	state = XLogLogicalInfoCtl->state;
	SpinLockRelease(&(XLogLogicalInfoCtl->mutex));

	if (state == LOGICAL_DECODING_STATE_READY)
		return;

	/*
	 * Get ready to sleep until the logical decoding gets activated.
	 * We may end up not sleeping, but we don't want to do this while
	 * holding the spinlock.
	 */
	ConditionVariablePrepareToSleep(&XLogLogicalInfoCtl->cv);

	for (;;)
	{
		SpinLockAcquire(&XLogLogicalInfoCtl->mutex);
		state = XLogLogicalInfoCtl->state;
		SpinLockRelease(&XLogLogicalInfoCtl->mutex);

		/* Return if the logical decoding is activated */
		if (state == LOGICAL_DECODING_STATE_READY)
			return;

		if (state == LOGICAL_DECODING_STATE_XLOG_LOGICALINFO)
		{
			/*
			 * Someone has already started the activation process. Wait
			 * for the activation to complete and check the status again.
			 */
			ConditionVariableSleep(&XLogLogicalInfoCtl->cv,
								   WAIT_EVENT_LOGICAL_DECODING_ACTIVATION);

			continue;
		}

		/* Activate logical decoding */
		ActivateLogicalDecoding();
	}

	ConditionVariableCancelSleep();
}

void
ActivateLogicalDecoding(void)
{
	bool recoveryInProgress = RecoveryInProgress();

	if (max_replication_slots == 0)
		return;

	if (IsTransactionState() &&
		GetTopTransactionIdIfAny() != InvalidTransactionId)
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				 errmsg("cannot activate logical decoding in transaction that has performed writes")));

	elog(LOG, "XXX start activating logical decoding");

	/*
	 * Get the latest status of logical info logging. If it's already
	 * activated, quick return.
	 */
	SpinLockAcquire(&XLogLogicalInfoCtl->mutex);
	if (XLogLogicalInfoCtl->state >= LOGICAL_DECODING_STATE_XLOG_LOGICALINFO)
	{
		elog(LOG, "XXX early return state %d", XLogLogicalInfoCtl->state);
		SpinLockRelease(&XLogLogicalInfoCtl->mutex);
		return;
	}

	/*
	 * Activate the logical info logging first.
	 */
	XLogLogicalInfoCtl->state = LOGICAL_DECODING_STATE_XLOG_LOGICALINFO;
	SpinLockRelease(&XLogLogicalInfoCtl->mutex);
	elog(LOG, "XXX enable logical info logging %d", XLogLogicalInfoCtl->state);

	PG_ENSURE_ERROR_CLEANUP(logical_decoding_activation_abort_callback, 0);
	{
		RunningTransactions running_xacts;

		elog(LOG, "XXX sent PROCSIGNAL_BARRIER_UPDATELOGICALINFO signal");
		WaitForProcSignalBarrier(
			EmitProcSignalBarrier(PROCSIGNAL_BARRIER_UPDATELOGICALINFO));

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
			 * Upper layers should prevent that we ever need to wait on ourselves.
			 * Check anyway, since failing to do so would either result in an
			 * endless wait or an Assert() failure.
			 */
			if (TransactionIdIsCurrentTransactionId(xid))
				elog(ERROR, "waiting for ourselves");

			elog(LOG, "XXX wait for xid %u to finish...", xid);
			XactLockTableWait(xid, NULL, NULL, XLTW_None);
		}

		elog(LOG, "XXX confirmed all txs finished!");
	}
	PG_END_ENSURE_ERROR_CLEANUP(logical_decoding_activation_abort_callback, 0);

	START_CRIT_SECTION();

	/* Activate logical decoding on the database cluster */
	SpinLockAcquire(&XLogLogicalInfoCtl->mutex);
	XLogLogicalInfoCtl->state = LOGICAL_DECODING_STATE_READY;
	SpinLockRelease(&XLogLogicalInfoCtl->mutex);

	elog(LOG, "XXX enable logical decoding!!");

	if (XLogStandbyInfoActive() && !recoveryInProgress)
	{
		bool	enabled = true;

		XLogBeginInsert();
		XLogRegisterData((char *) (&enabled), sizeof(bool));

		XLogInsert(RM_XLOG_ID, XLOG_LOGICAL_DECODING_STATE);
	}

	END_CRIT_SECTION();

	/* Let everybody know we've activated the logical decoding */
	ConditionVariableBroadcast(&XLogLogicalInfoCtl->cv);
}

static void
logical_decoding_activation_abort_callback(int code, Datum arg)
{
	SpinLockAcquire(&XLogLogicalInfoCtl->mutex);
	XLogLogicalInfoCtl->state = LOGICAL_DECODING_STATE_DISABLED;
	SpinLockRelease(&XLogLogicalInfoCtl->mutex);

	/* Let everybody know we failed to activate the logical decoding */
	ConditionVariableBroadcast(&XLogLogicalInfoCtl->cv);
}

Datum
pg_activate_logical_decoding(PG_FUNCTION_ARGS)
{
	EnsureLogicalDecodingActive();

	PG_RETURN_VOID();
}

Datum
pg_deactivate_logical_decoding(PG_FUNCTION_ARGS)
{
	bool deactivated;
	int	active_logical_slots = 0;
	bool recoveryInProgress = RecoveryInProgress();

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

		active_logical_slots++;
	}

	if (active_logical_slots > 0)
		ereport(ERROR,
				(errmsg("cannot deactivate logical decoding while having valid logical replication slots")));

	if (XLogStandbyInfoActive() && !recoveryInProgress)
	{
		bool	enabled = false;

		XLogBeginInsert();
		XLogRegisterData((char *) (&enabled), sizeof(bool));

		XLogInsert(RM_XLOG_ID, XLOG_LOGICAL_DECODING_STATE);
	}

	UpdateLogicalDecodingState(false);

	LWLockRelease(ReplicationSlotAllocationLock);

	PG_RETURN_BOOL(deactivated);
}

Datum
pg_get_logical_decoding_state(PG_FUNCTION_ARGS)
{
	LogicalDecodingState state;
	const char *state_str = "unknown";

	SpinLockAcquire(&(XLogLogicalInfoCtl->mutex));
	state = XLogLogicalInfoCtl->state;
	SpinLockRelease(&(XLogLogicalInfoCtl->mutex));

	switch (state)
	{
		case LOGICAL_DECODING_STATE_DISABLED:
			state_str = "disabled";
			break;
		case LOGICAL_DECODING_STATE_XLOG_LOGICALINFO:
			state_str = "xlog-logicalinfo";
			break;
		case LOGICAL_DECODING_STATE_READY:
			state_str = "ready";
			break;
	}

	PG_RETURN_TEXT_P(cstring_to_text(state_str));
}

