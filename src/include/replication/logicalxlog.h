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

/*
 * The status of WAL-logging logical info and logical decoding.
 */
typedef enum LogicalDecodingStatus
{
	LOGICAL_DECODING_STATUS_DISABLED = 0,
	LOGICAL_DECODING_STATUS_XLOG_LOGICALINFO,
	LOGICAL_DECODING_STATUS_READY,
}			LogicalDecodingStatus;

typedef struct XLogLogicalInfoCtlData XLogLogicalInfoCtlData;
extern XLogLogicalInfoCtlData *XLogLogicalInfoCtl;
extern bool XLogLogicalInfo;

extern Size LogicalXlogShmemSize(void);
extern void LogicalXlogShmemInit(void);
extern void StartupLogicalDecodingStatus(bool enabled_at_last_checkpoint);
extern bool XLogLogicalInfoEnabled(void);
extern bool IsLogicalDecodingActive(void);
extern void UpdateLogicalDecodingStatus(bool enabled);

#endif							/* LOGICALXLOG_H */
