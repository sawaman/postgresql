/*-------------------------------------------------------------------------
 * logicalxlog.h
 *	  Exports from replication/logical/logicalxlog.c.
 *
 * Copyright (c) 2012-2024, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOGICALXLOG_H
#define LOGICALXLOG_H

typedef enum LogicalDecodingState
{
	LOGICAL_DECODING_STATE_DISABLED = 0,
	LOGICAL_DECODING_STATE_XLOG_LOGICALINFO,
	LOGICAL_DECODING_STATE_READY,
} LogicalDecodingState;

extern bool	XLogLogicalInfo;

extern Size LogicalXlogShmemSize(void);
extern void LogicalXlogShmemInit(void);
extern void StartupLogicalDecodingState(bool enabled_at_last_checkpoint);
extern bool ProcessBarrierUpdateLogicalInfo(void);
extern bool IsXLogLogicalInfoActive(void);
extern bool IsLogicalDecodingActive(void);
extern void UpdateLogicalDecodingState(bool enabled);
extern void EnsureLogicalDecodingActive(void);
extern void ActivateLogicalDecoding(void);

#endif							/* LOGICALXLOG_H */

