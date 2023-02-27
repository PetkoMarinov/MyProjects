DECLARE @call_Id INT = 3803745

DECLARE @showCallee BIT = 0
DECLARE @showTblCalls BIT = 0
DECLARE @showStatistics BIT = 0
DECLARE @showExplanation BIT = 0

--DECLARE @FromDate DATETIME = '2022-11-01', @ToDate DATETIME ='2022-11-30'	


DECLARE @table AS TABLE
(
	Call_Id INT NOT NULL,
	S_Id INT ,
	CalleeStartTime DATETIME2(0),
	CalleeAnswerTime DATETIME2(0),
	CalleeEndTime DATETIME2(0),
	S_StartTime DATETIME2(0),
	S_EndTime DATETIME2(0),
	[Action] INT NOT NULL,
	Ringing INT,
	Talking INT,
	Ringing2 INT,
	Talking2 INT,
	Src_Id BIGINT NOT NULL,
	Caller NVARCHAR(100),
	CallerName NVARCHAR(100),
	CType INT,
	Dst_Id BIGINT NOT NULL,
	ActionParty BIGINT,
	Callee NVARCHAR(100),
	CalleeName NVARCHAR(100),
	CalleeType INT,
	CalleeGroup NVARCHAR(100),
	Call_IsA BIT NOT NULL,
	S_IsA INT,
	S_A_Inb INT,
	S_A_Outb INT,
	S_from_Int INT,
	S_to_Int INT,
	S_Seq INT NOT NULL,
	S_Group INT NOT NULL,
	S_Type INT NOT NULL,
	Q_Id INT,
	Q_IsA INT,
	Q_IsUnA INT,
	Q_Waiting INT,
	Q_Polling INT,
	Q_Talking INT,
	Q_HistoryId VARCHAR(100),
	Q_Polls INT,
	Q_Dialed INT,
	Q_Reason_NoA INT,
	Q_Reason_fail INT,
	[ROW] INT 
)

INSERT INTO @table SELECT 
	c.id AS Call_Id,
	cs.id AS S_Id,
	CAST(cp1.start_time AS DATETIME2(0)) AS CalleeStartTime,			
	CAST(cp1.answer_time AS DATETIME2(0)) AS CalleeAnswerTime,	
	CAST(cp1.end_time AS DATETIME2(0)) AS CalleeEndTime,
	CAST(cs.start_time AS DATETIME2(0)) AS S_StartTime,	
	CAST(cs.end_time AS DATETIME2(0)) AS S_EndTime,
	cs.action_id AS [Action],
	CASE WHEN cpa1.dn_type <> 4 AND (cp1.answer_time IS NULL OR cp1.answer_time > cp1.end_time) THEN NULLIF(DATEDIFF(second, cs.start_time, cs.end_time), 0)
		 WHEN cpa1.dn_type = 4 AND ISNULL(cp1.answer_time,0) <= c.end_time THEN NULLIF(DATEDIFF(second, cs.start_time, cs.end_time), 0) END AS Ringing,
	CASE WHEN cpa1.dn_type <> 4 AND cp1.answer_time IS NOT NULL THEN
		CASE WHEN cp1.answer_time < cp1.start_time THEN DATEDIFF(second, cp1.start_time, cp1.end_time)
			 WHEN cp1.answer_time <= cp1.end_time AND cp1.end_time != cs.end_time THEN NULLIF(DATEDIFF(second, cs.start_time, cs.end_time), 0) --45199 3cx_AirGroupLlc segment 189549
			 WHEN cp1.answer_time <= cp1.end_time THEN DATEDIFF(second, cp1.answer_time, cp1.end_time) 
		END
	END AS Talking,
	CASE WHEN cpa1.dn_type <> 4 AND cp1.answer_time IS NULL OR cp1.answer_time > cp1.end_time THEN NULLIF(DATEDIFF(second, cs.start_time, cs.end_time), 0)
		 WHEN cpa1.dn_type = 4 AND ISNULL(cp1.answer_time,0) <= c.end_time THEN NULLIF(DATEDIFF(second, cs.start_time, cs.end_time), 0) END AS Ringing2,
	CASE WHEN cpa1.dn_type <> 4 AND cp1.answer_time IS NOT NULL AND cp1.answer_time <= cp1.end_time THEN NULLIF(DATEDIFF(second, cs.start_time, cs.end_time), 0) END AS Talking2,
	cs.src_part_id AS Src_Id,			
	cpa.caller_number AS Caller,			
	cpa.caller_number_display AS CallerName,			
	cpa.dn_type AS CallerType,			
	cs.dst_part_id AS Dst_Id,
	cs.action_party_id AS ActionParty,
	cpa1.caller_number AS Callee,	
	cpa1.caller_number_display AS CalleeName,
	cpa1.dn_type AS CalleeType,
	e.DNType AS CalleeGroup,			
	c.is_answered AS Call_IsA, 		
	cs.is_answered AS S_IsA,			
	cs.is_answered_inbound AS S_A_Inb, -- S = Segment			
	cs.is_answered_outbound AS S_A_Outb,			
	cs.is_from_internal AS S_from_Int,			
	cs.is_to_internal AS S_to_Int,			
	cs.seq_order AS S_Seq,			
	cs.seq_group AS S_Group,			
	cs.[type] AS S_Type
	,qc.idcallcent_queuecalls AS Q_Id,
	qc.is_answered AS Q_IsA,			
	qc.is_unanswered AS Q_IsUnA,			
	qc.ts_waiting AS Q_Waiting,			
	qc.ts_polling AS Q_Polling,			
	qc.ts_servicing AS Q_Talking,	
	qc.call_history_id AS Q_HistoryId,			
	qc.count_polls AS Q_Polls,			
	qc.count_dialed AS Q_Dialed,			
	qc.reason_noanswercode AS Q_Reason_NoA,			
	qc.reason_failcode AS Q_Reason_fail,			
	ROW_NUMBER() OVER (ORDER BY c.id, cs.start_time, cs.id, cs.seq_order) AS ROW
FROM tblCalls c				
LEFT JOIN tblCallSegments cs ON cs.call_id = c.id
LEFT JOIN tblCallParticipants cp ON cp.id = cs.src_part_id				
LEFT JOIN tblCallParticipants cp1 ON cp1.id = cs.dst_part_id
LEFT JOIN tblCallParties cpa ON cpa.id = cp.info_id				
LEFT JOIN tblCallParties cpa1 ON cpa1.id = cp1.info_id				
LEFT JOIN tblExtensions e ON e.ExtensionNumber = cpa1.caller_number			
LEFT JOIN tblQueueCalls qc ON qc.q_num = cpa1.caller_number 
	AND qc.time_end <= c.end_time
	AND cp1.answer_time IS NOT NULL 
	AND cs.type = 2
	AND (ABS(DATEDIFF(SECOND,qc.time_start,cs.start_time)) <= 1 OR (qc.time_start = cp1.start_time)) 
	AND ((qc.from_userpart <> 'Anonymous' AND qc.from_userpart= cpa.caller_number)
		OR 
		qc.from_userpart = 'Anonymous')
WHERE --qc.call_history_id = '00000C1C9E09473A_1621'
	c.id = @call_Id
	
ORDER BY c.id, cs.start_time, cs.id, cs.seq_order

------------------------------------------------------------------------------------------------------------------------------------------------------------------

DECLARE @totalRows INT = (SELECT COUNT(*) FROM @table) -- total rows in the query result                                                                           
DECLARE @rowCounter INT = 1																								
DECLARE @queuesProcessed TABLE (N INT, Queue INT, QueueRow INT, ActionParty BIGINT, ActionPartyRow INT, IsAnswered BIT) -- table, which contains queues that appear in the query result
DECLARE @queueCounter INT = 0

WHILE @rowCounter <= @totalRows
BEGIN
		--SEARCHING FOR QUEUE CALLS AND REGISTERING INTO A TABLE

	DECLARE @queueIsAnswered BIT = (SELECT Q_IsA FROM @table WHERE [ROW] = @rowCounter)
	DECLARE @queueIsUnanswered BIT = (SELECT Q_IsUnA FROM @table WHERE [ROW] = @rowCounter)
		
	IF @queueIsAnswered = 1 OR @queueIsUnanswered = 1
	BEGIN
		SET @queueCounter += 1

		DECLARE @actionParty BIGINT = (SELECT ActionParty FROM @table WHERE [ROW] = @rowCounter AND (Q_IsA = 1 OR Q_IsUnA = 1))
		DECLARE @actionPartyRow BIGINT = (SELECT TOP 1 [ROW] FROM @table WHERE Dst_Id = @actionParty)

		INSERT INTO @queuesProcessed 
			SELECT @queueCounter, Callee, [ROW], ActionParty, @actionPartyRow, CASE WHEN @queueIsAnswered = 1 THEN 1 ELSE 0 END
			FROM @table WHERE [ROW] = @rowCounter
	END
	--ELSE IF (SELECT CASE WHEN CalleeType = 4 AND [ROW] = @rowCounter AND CalleeAnswerTime IS NULL AND Q_Id IS NULL AND S_Type = 1 THEN 1 ELSE 0 END 
	--		 FROM @table WHERE CalleeType = 4 AND [ROW] = @rowCounter) = 1
	--BEGIN 
	--	DELETE FROM @table WHERE [ROW] = @rowCounter
	--END

		-- ALERT IN CASE THERE IS QUEUE (4) AS PART OF THE CALL, BUT IT WAS NOT REGISTERED IN tblQueueCalls	
	UPDATE @table
	SET CalleeName = CASE WHEN CalleeType = 4 AND [ROW] = @rowCounter AND CalleeAnswerTime IS NOT NULL AND  Q_Id IS NULL AND S_Type = 2 THEN 'No Q_history' ELSE CalleeName END
					 FROM @table WHERE CalleeType = 4 AND [ROW] = @rowCounter

	SET @rowCounter += 1
END

--SELECT * FROM @queuesProcessed
--SELECT * FROM @table

-- DELETION THE LINES WITH INFORMATION ABOUT THE CALLED AGENTS (Polling agents in Queue)
------ DEFINING WHICH ROWS SHOULD BE DELETED

WHILE @queueCounter > 0
BEGIN

	DECLARE @queueRow INT = (SELECT QueueRow FROM @queuesProcessed WHERE N = @queueCounter)
	DECLARE @partyRow INT = (SELECT ActionPartyRow FROM @queuesProcessed WHERE N = @queueCounter)
	DECLARE @deletePollingAgentsFrom INT 
	DECLARE @deletePollingAgentsTo INT 

	IF (SELECT IsAnswered FROM @queuesProcessed WHERE N = @queueCounter) = 1
	BEGIN
		SET @deletePollingAgentsFrom = @queueRow + 1
		SET @deletePollingAgentsTo = @partyRow - 1

		DECLARE @precedingRows INT = @deletePollingAgentsFrom - 1   -- all rows before queue row

		WHILE @precedingRows > 1        --- rows between first row and @deletePollingAgentsFrom row  -- 3196452  --3cx_333Help
		BEGIN
			DECLARE @answerTime DATETIME2(0) = (SELECT CalleeAnswerTime FROM @table WHERE [ROW] = @precedingRows)
			DECLARE @startTime DATETIME2(0) = (SELECT S_StartTime FROM @table WHERE [ROW] = @precedingRows)

			IF (SELECT Action FROM @table WHERE [ROW] = @precedingRows) IN (10, 15)
				AND @answerTime IS NULL
				AND @startTime = (SELECT S_StartTime FROM @table WHERE [ROW] = @deletePollingAgentsFrom)

				DELETE FROM @table 
				WHERE [ROW] = @precedingRows;
		
		SET @precedingRows -= 1
		END
		
	END
		
	ELSE IF @partyRow IS NOT NULL
	BEGIN
		SET @deletePollingAgentsFrom = @queueRow + 1
		DECLARE @interimParty INT = (SELECT COUNT(DISTINCT ActionParty) FROM @table WHERE [ROW] > @queueRow AND [ROW] < @partyRow)

		IF @interimParty IS NULL SET @deletePollingAgentsTo = @partyRow - 1
		ELSE SET @deletePollingAgentsTo = @interimParty - 1;

	END
	ELSE
	BEGIN
		DECLARE @furtherActionPartyExists INT = (SELECT COUNT(DISTINCT ActionParty) FROM @table WHERE [ROW] > @queueRow)

		SET @deletePollingAgentsFrom = @queueRow + 1

		IF @furtherActionPartyExists = 0
			SET @deletePollingAgentsTo = @totalRows; --check
	END

	---------- DELETION ITSELF ---------------------------
	DELETE FROM @table 
	WHERE [ROW] BETWEEN @deletePollingAgentsFrom AND @deletePollingAgentsTo
		
	SET @queueCounter -= 1
END

-- RENUMBERING THE LINES AFTER DELETION
DECLARE @temp AS TABLE
(Call_Id INT NOT NULL,S_Id INT NOT NULL,CalleeStartTime DATETIME2(0),CalleeAnswerTime DATETIME2(0),CalleeEndTime DATETIME2(0),S_StartTime DATETIME2(0),S_EndTime DATETIME2(0),[Action] INT NOT NULL,Ringing INT,Talking INT, Ringing2 INT, Talking2 INT, Src_Id BIGINT NOT NULL,Caller NVARCHAR(100),CallerName NVARCHAR(100),CType INT,Dst_Id BIGINT NOT NULL,ActionParty BIGINT,Callee NVARCHAR(100),CalleeName NVARCHAR(100),CalleeType INT,CalleeGroup NVARCHAR(100),Call_IsA BIT NOT NULL,S_IsA INT,S_A_Inb INT,S_A_Outb INT,S_from_Int INT,S_to_Int INT,S_Seq INT NOT NULL,S_Group INT NOT NULL,S_Type INT NOT NULL,Q_Id INT,Q_IsA INT,Q_IsUnA INT,Q_Waiting INT,Q_Polling INT,Q_Talking INT,Q_HistoryId VARCHAR(100),Q_Polls INT,Q_Dialed INT,Q_Reason_NoA INT,Q_Reason_fail INT,[ROW] INT);
	
WITH CTE AS
(
	SELECT 
		Call_Id,S_Id,CalleeStartTime,CalleeAnswerTime,CalleeEndTime,S_StartTime,S_EndTime,Action,Ringing,Talking, Ringing2, Talking2, Src_Id,Caller,CallerName,CType,Dst_Id,ActionParty,Callee,CalleeName,CalleeType,CalleeGroup,Call_IsA,S_IsA,S_A_Inb,S_A_Outb,S_from_Int,S_to_Int,S_Seq,S_Group,S_Type,Q_Id,Q_IsA,Q_IsUnA,Q_Waiting,Q_Polling,Q_Talking,Q_HistoryId,Q_Polls,Q_Dialed,Q_Reason_NoA,Q_Reason_fail,
		ROW_NUMBER() OVER (ORDER BY [ROW]) AS [Row]
	FROM @table
) INSERT INTO @temp SELECT * FROM CTE
		
DELETE FROM @table
INSERT INTO @table SELECT * FROM @temp


-- SWAPPING PLACES OF THE LAST TWO ROWS
---- WHEN cs.start_time of penult row + 1 = cs.start_time of last row, BUT cs.end_time of penult row > cs.end_time of last row
--DECLARE @beforeLast_StartTime DATETIME2(0) = (SELECT S_StartTime FROM @table WHERE [ROW] = (SELECT COUNT(*) FROM @table) - 1)
--DECLARE @beforeLast_EndTime DATETIME2(0) = (SELECT S_EndTime FROM @table WHERE [ROW] = (SELECT COUNT(*) FROM @table) - 1)
	
--DECLARE @lastRow_StartTime DATETIME2(0) = (SELECT S_StartTime FROM @table WHERE [ROW] = (SELECT COUNT(*) FROM @table))
--DECLARE @lastRow_EndTime DATETIME2(0) = (SELECT S_EndTime FROM @table WHERE [ROW] = (SELECT COUNT(*) FROM @table))

--IF DATEDIFF(SECOND,@beforeLast_StartTime,@lastRow_StartTime) = 1 
--	AND @beforeLast_EndTime > @lastRow_EndTime
--BEGIN
--	UPDATE t
--	SET [ROW] -= 1
--	FROM @table t
--	WHERE [ROW] = (SELECT COUNT(*) FROM @table)

--	UPDATE t
--	SET [ROW] += 1
--	FROM @table t
--	WHERE [ROW] = (SELECT COUNT(*) FROM @table) - 1 
--		AND S_EndTime = @beforeLast_EndTime 
--END


-- IN CASES WHERE THE ID NUMBER IN COLUMN "ID" IN TBLCALLSEGMENTS IS INCORRECTLY ATTACHED. (192542 3cx_Florajet)

DECLARE @firstSegmentStartTime DATETIME2(0) = (SELECT S_StartTime FROM @table WHERE [ROW] = 1)
DECLARE @lastRow INT = (SELECT MAX(ROW) FROM @table)
DECLARE @lastSegmentStartTime DATETIME2(0) = (SELECT S_EndTime FROM @table WHERE [ROW] = @lastRow)

IF DATEDIFF(DAY, @firstSegmentStartTime, @lastSegmentStartTime) > 0 
	AND DATEDIFF(SECOND, @firstSegmentStartTime, @lastSegmentStartTime) > 7200 -- two hours
BEGIN	
	DECLARE @deleteSegmentsFromRow INT =
	(SELECT TOP 1 [ROW] + 1
	FROM 
		(SELECT S_StartTime, LEAD(S_StartTime) OVER (ORDER BY [ROW]) AS NextSegmentStartTime, [ROW]
		FROM @table) t
	WHERE DATEDIFF(DAY, S_StartTime, NextSegmentStartTime) > 0 )

	DELETE FROM @table WHERE [ROW] >= @deleteSegmentsFromRow
END		

	DECLARE @currentSegmentEndTime DATETIME2(0) = (SELECT S_EndTime FROM @table WHERE [ROW] = @rowCounter)
	DECLARE @nextSegmentStartTime DATETIME2(0) = (SELECT S_StartTime FROM @table WHERE [ROW] = @rowCounter + 1)

----------------------------------------------------------------------------------------------------------------------------------

SET @rowCounter = 1
SET @totalRows = (SELECT COUNT(*) FROM @table)

WHILE @rowCounter <= @totalRows
BEGIN
	DECLARE @currentSegmentStartTime DATETIME2(0) = (SELECT S_StartTime FROM @table WHERE [ROW] = @rowCounter)
	DECLARE @previousSegmentEndTime DATETIME2(0) = (SELECT S_EndTime FROM @table WHERE [ROW] = @rowCounter - 1)
	DECLARE @isCallerIVR BIT = IIF((SELECT CType FROM @table WHERE [ROW] = @rowCounter) = 6,1,0)

	SET @currentSegmentEndTime = (SELECT S_EndTime FROM @table WHERE [ROW] = @rowCounter)
	SET @nextSegmentStartTime = (SELECT S_StartTime FROM @table WHERE [ROW] = @rowCounter + 1)

	DECLARE @precedingSegmentsCounter INT = 1

	WHILE @precedingSegmentsCounter < @rowCounter
	BEGIN 
		DECLARE @precedingSegmentStartTime DATETIME2(0) = (SELECT S_StartTime FROM @table WHERE [ROW] = @precedingSegmentsCounter)
		DECLARE @precedingSegmentEndTime DATETIME2(0) = (SELECT S_EndTime FROM @table WHERE [ROW] = @precedingSegmentsCounter)
		DECLARE @currentCalleeEndTime DATETIME2(0) = (SELECT CalleeEndTime FROM @table WHERE [ROW] = @rowCounter)
		DECLARE @precedingCalleeEndTime DATETIME2(0) = (SELECT CalleeEndTime FROM @table WHERE [ROW] = @precedingSegmentsCounter)
		

		IF @currentSegmentEndTime <= @precedingSegmentEndTime
			OR (@currentSegmentEndTime > @precedingSegmentEndTime AND DATEDIFF(SECOND, @previousSegmentEndTime, @currentSegmentStartTime) > 1)  -- 910033 - 3cx_IsaacHeating, 1934355 3cx_LenThePlumber
		--	OR (@currentSegmentEndTime > @precedingSegmentEndTime AND DATEDIFF(SECOND, @precedingSegmentEndTime, @currentSegmentStartTime) = 1)
		
		BEGIN
	
			UPDATE t
			SET Ringing2 = NULL
			FROM @table t
			WHERE [ROW] = @rowCounter

			UPDATE t
			SET Talking2 = NULL
			FROM @table t
			WHERE [ROW] = @rowCounter
		END

		IF DATEDIFF(SECOND, @currentSegmentStartTime, @precedingSegmentEndTime) > 1 
			AND @currentSegmentEndTime > @precedingSegmentEndTime
			AND DATEDIFF(SECOND, @precedingSegmentEndTime, @currentSegmentEndTime) > 1  -- 3cx_LenThePlumber 1024583; 63766 - 3cx_AirGroupLlc

		BEGIN
				UPDATE t
				SET Talking2 = Talking - DATEDIFF(SECOND, @currentSegmentStartTime, @precedingSegmentEndTime)
				FROM @table t
				WHERE [ROW] = @rowCounter 
		END
		ELSE IF ABS(DATEDIFF(SECOND, @currentSegmentStartTime, @precedingSegmentStartTime)) <= 1
			AND DATEDIFF(SECOND, @precedingSegmentEndTime, @currentSegmentEndTime) > 1 -- 910033 - 3cx_IsaacHeating 
			AND DATEDIFF(SECOND, @currentSegmentEndTime, @precedingSegmentStartTime) <= 7200 -- 192542 3cx_Florajet
		BEGIN 
			DECLARE @maxCalleeEndTimeBiggestUpToNow DATETIME2 = (SELECT MAX(CalleeEndTime) FROM @table WHERE [ROW] BETWEEN 1 AND @rowCounter - 1)
				
			IF @currentCalleeEndTime > @maxCalleeEndTimeBiggestUpToNow -- 1934355 3cx_LenThePlumber
			BEGIN
				UPDATE t
				SET Talking2 = Talking--- DATEDIFF(SECOND, @currentSegmentStartTime, @precedingSegmentEndTime)
				FROM @table t
				WHERE [ROW] = @rowCounter 

			END
		END
		SET @precedingSegmentsCounter += 1
	END

	SET @rowCounter += 1
END

SELECT * FROM @table --ORDER BY S_Id (row 340)

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------

IF @showCallee = 1 
BEGIN
	SELECT 			
		cp1.id AS CalleeId,	
		e.ExtensionNumber AS Callee,
		cpa1.caller_number_display AS CalleeName,
		cp1.call_id AS Call_Id,		
		cp1.role AS CP_Role,		
		cs.action_id,	
		cp1.forward_reason AS CP_Forward_Reason,		
		cp1.failure_reason AS CP_Failure_Reason,
		cp1.end_status AS CP_End_Status,	
		cpa1.dn_type AS CP_DnType,	
		e.ExtensionTypeID AS E_DnType, 		
		e.DNType AS E_DnTypeDesc,
		e.QueueStatus,
		e.IsBindToMSec,
		e.NoAnswerTimeout,	
		e.IsInternal AS E_Is_Internal,
		cp1.is_inbound AS CP_Is_Inb,	
		cpa1.internal_dn AS CPA_IsInternal_Dn, 		
		cpa1.external_dn AS CPA_IsExternal_Dn		
	FROM tblCalls c			
	LEFT JOIN tblCallSegments cs ON cs.call_id = c.id			
	LEFT JOIN tblCallParticipants cp1 ON cp1.id = cs.dst_part_id			
	LEFT JOIN tblCallParties cpa1 ON cpa1.id = cp1.info_id			
	LEFT JOIN tblExtensions e ON e.ExtensionNumber = cpa1.caller_number			
	WHERE cp1.call_id = @call_Id 
		--AND cp1.id IN (SELECT Dst_Id FROM @table)-- 	slow performance
	ORDER BY c.id, cs.start_time, cs.end_time, cs.seq_order

	--SELECT 			
	--	cp.id AS CallerId,		
	--	cp.call_id AS Call_Id,		
	--	cp.role AS CP_Role,		
	--	cp.failure_reason AS CP_Failure_Reason,		
	--	cpa.dn_type AS CP_DnType,		
	--	e.DNType AS E_DnTypeDesc,		
	--	e.ExtensionTypeID AS E_DnType,
	--	cp.is_inbound AS CP_Is_Inb,		
	--	e.IsInternal AS E_Is_Internal,		
	--	cpa.internal_dn AS CPA_IsInternal_Dn,		
	--	cpa.external_dn AS CPA_IsExternal_Dn		
	--FROM tblCalls c			
	--LEFT JOIN tblCallSegments cs ON cs.call_id = c.id			
	--LEFT JOIN tblCallParticipants cp ON cp.id = cs.src_part_id			
	--LEFT JOIN tblCallParties cpa ON cpa.id = cp.info_id			
	--LEFT JOIN tblExtensions e ON e.ExtensionNumber = cpa.caller_number			
	--WHERE cp.call_id = @call_Id -- CALLER		
	--	AND cp.failure_reason != 1
END

IF @showTblCalls = 1
BEGIN
	DECLARE @tblCalls_EndTimeMinusStartTime INT = (SELECT DATEDIFF(s, start_time, end_time) FROM tblCalls WHERE id = @call_Id) 
	DECLARE @tblCalls_DeclaredTotalTime INT = (SELECT ringing_dur + talking_dur + q_wait_dur FROM tblCalls WHERE id = @call_Id) 

	SELECT
		Id AS tblCalls_Id,
		start_time,
		end_time,
		is_answered,
		ringing_dur,
		talking_dur,
		q_wait_dur,
		@tblCalls_DeclaredTotalTime AS tblCallsTime, 
		@tblCalls_EndTimeMinusStartTime AS [EndTime minus StartTime]
	FROM tblCalls c WHERE c.Id = @call_Id 
END


IF @showStatistics = 1
BEGIN
	DECLARE @tblCalls_Statistics TABLE
	(
		InboundCall BIT,
		OutboundCall BIT,
		IsAnswered BIT,
		IsAnsweredInbound BIT,
		IsAnsweredOutbound BIT,
		RingingTime INT,
		TalkingTime INT,
		Total INT
	)

	INSERT INTO @tblCalls_Statistics
		SELECT 
			CASE WHEN (SELECT CType FROM @table WHERE [ROW] = 1) = 1 THEN 1 ELSE 0 END AS InboundCall,
			CASE WHEN (SELECT CType FROM @table WHERE [ROW] = 1) IN (0,5,6) THEN 1 ELSE 0 END AS OutboundCall,
			CASE WHEN (SELECT Call_IsA FROM @table WHERE [ROW] = 1) = 1 THEN 1 ELSE 0 END AS IsAnswered,
			CASE WHEN EXISTS(SELECT 1 FROM @table WHERE Talking > 0 AND CalleeType = 0) THEN 1 ELSE 0 END AS IsAnsweredInbound,
			CASE WHEN EXISTS(SELECT 1 FROM @table WHERE Talking > 0 AND CType = 0 AND CalleeType NOT IN (5,6)) THEN 1 ELSE 0 END AS IsAnsweredOutbound,
			ISNULL(SUM(Ringing2), 0) AS RingingTime,
			ISNULL(SUM(Talking2), 0) AS TalkingTime,
			ISNULL(SUM(Ringing2), 0) +ISNULL(SUM(Talking2), 0) AS Total
		FROM @table

	SELECT * FROM @tblCalls_Statistics
END

IF @showExplanation = 1
BEGIN
	DECLARE @actionTable TABLE (Id INT, [Name] VARCHAR (200))
	
	INSERT INTO @actionTable VALUES
		(1, 'connected'),
		(2, 'picked up by'),
		(5, 'terminated by'),
		(6, 'terminated by'),
		(7, 'transfer failed'),
		(8,'transfer failed'),
		(9, 'who was replaced by'),
		(10, 'who was replaced by'),
		(11, 'return back to'),
		(12, 'was joined'),
		(13, 'abandoned, sent to'),
		(15, 'queue polling agent'),
		(101, 'no answer, forwarded to'),
		(102, 'busy, forwarded to'),
		(103, 'not registered, forwarded to'),
		(104, 'forwarded to'),
		(400, 'failed'),
		(403, 'failed'),
		(404, 'caller forbidden'),
		(405, 'caller forbidden'),
		(406, 'limited by license'),
		(407, 'line is busy'),
		(408, 'target not found'),
		(409, 'terminated by rule'),
		(410, 'terminated by rule'),
		(411, 'forwarding loop'),
		(412, 'Target is busy'),
		(413, 'No answer timeout'),
		(414, 'user terminated'),
		(415, 'not available'),
		(416, 'not registered'),
		(417, 'no route exists'),
		(418, 'failed canceled'),
		(419, 'redirected to'),
		(420, 'server error'),
		(421, 'route busy'),
		(422, 'route not found'),
		(423, 'route disabled'),
		(425, 'target disabled'),
		(426, 'external calls disabled'),
		(430, 'forward failed (no outbound rule)'),
		(432, 'external call forbidden'),
		(999999,'')
		
	SET @rowCounter = 1
	SET @totalRows = (SELECT MAX(ROW) FROM @table)
	DECLARE @explanationString VARCHAR(MAX)= 
		'Call started at ' + FORMAT((SELECT S_StartTime FROM @table WHERE [ROW] = 1),'dd/MM/yyyy hh:mm:ss') + CHAR(13)

	WHILE @rowCounter < @totalRows
	BEGIN
		IF (SELECT  COALESCE(Ringing, Talking, Ringing2, Talking2) FROM @table WHERE [ROW] = @rowCounter) IS NOT NULL
		BEGIN
			DECLARE @actionName VARCHAR(200) = (SELECT [Name] FROM @actionTable WHERE Id = (SELECT Action FROM @table WHERE [ROW] = @rowCounter))

			DECLARE @caller VARCHAR (200) = CONVERT(VARCHAR, (SELECT CallerName FROM @table WHERE [ROW] = @rowCounter))
			DECLARE @callee VARCHAR (200) = CONVERT(VARCHAR, (SELECT CalleeName FROM @table WHERE [ROW] = @rowCounter))
			DECLARE @nextSegmentCallee VARCHAR(200) = CONVERT(VARCHAR, (SELECT CalleeName FROM @table WHERE [ROW] = @rowCounter + 1))
			DECLARE @calleeType VARCHAR (200) = CONVERT(VARCHAR, (SELECT CalleeType FROM @table WHERE [ROW] = @rowCounter))
		
			DECLARE @ringingTime INT = 
				CASE WHEN (SELECT Ringing2 FROM @table WHERE [ROW] = @rowCounter) IS NULL THEN (SELECT Ringing FROM @table WHERE [ROW] = @rowCounter)
				     WHEN (SELECT Ringing FROM @table WHERE [ROW] = @rowCounter) = (SELECT Ringing2 FROM @table WHERE [ROW] = @rowCounter)
						THEN (SELECT Ringing FROM @table WHERE [ROW] = @rowCounter) ELSE 0 
				END

			DECLARE @talkingTime INT = 
				CASE WHEN (SELECT Talking2 FROM @table WHERE [ROW] = @rowCounter) IS NULL 
						AND (SELECT Talking2 FROM @table WHERE [ROW] = @rowCounter) IS NOT NULL THEN (SELECT Talking FROM @table WHERE [ROW] = @rowCounter)
				     WHEN (SELECT Talking FROM @table WHERE [ROW] = @rowCounter) = (SELECT Talking2 FROM @table WHERE [ROW] = @rowCounter)
						THEN (SELECT Talking FROM @table WHERE [ROW] = @rowCounter) ELSE 0 
				END

			DECLARE @ringingMinutes INT = IIF(@ringingTime != 0, @ringingTime / 60, 0)
			DECLARE @ringingSeconds INT = IIF(@ringingTime != 0, @ringingTime % 60, 0)
			DECLARE @talkingHours INT = IIF(@talkingTime != 0, @talkingTime / 3600, 0)
			DECLARE @talkingMinutes INT = CASE WHEN @talkingHours = 0 THEN @talkingTime / 60
											   WHEN @talkingHours != 0 THEN (@talkingTime - @talkingHours * 3600) / 60
										  END

			DECLARE @talkingSeconds INT = CASE WHEN @talkingHours = 0 AND @talkingMinutes = 0 THEN @talkingTime
												   WHEN @talkingHours != 0 AND @talkingMinutes = 0 THEN @talkingTime - @talkingHours * 3600
												   WHEN @talkingHours = 0 AND @talkingMinutes != 0 THEN @talkingTime - @talkingMinutes * 60
																								   ELSE @talkingTime 
										  END
								
			SET @explanationString += 
			(
				CHAR(9) + @caller + ' ' + 
				CASE WHEN @ringingTime IS NOT NULL AND @ringingTime != 0 THEN 'rang to ' + @callee  
					 WHEN @calleeType = 6 AND @talkingTime IS NOT NULL THEN 'listened the instructions of ' + @callee + ' ' 
					 WHEN @talkingTime IS NOT NULL AND @talkingTime != 0 AND @calleeType NOT IN (8,9) THEN 'talked with ' + @callee
					 WHEN @talkingTime IS NOT NULL AND @talkingTime != 0 AND @calleeType = 8 THEN 'waited in '  + @callee + ' ' 
					 WHEN @talkingTime IS NOT NULL AND @talkingTime != 0 AND @calleeType = 9 THEN 'participated in ' ELSE '' END + ' ' +
				@actionName  + ' ' +
				@nextSegmentCallee +
				' (' + 
				CASE WHEN @ringingMinutes = 0 AND @ringingSeconds = 0 THEN '' 
					 WHEN @ringingMinutes = 0 THEN CAST(@ringingSeconds AS VARCHAR) + ' s ringing' 
					 WHEN @ringingMinutes != 0 THEN CAST(@ringingMinutes AS VARCHAR) + ' m ' + CAST(@ringingSeconds AS VARCHAR) + ' s ringing' 
				END +
				CASE WHEN @talkingHours = 0 AND @talkingMinutes = 0 AND @talkingSeconds = 0 THEN '' 
					 WHEN @talkingHours != 0 AND @talkingMinutes = 0 AND @talkingSeconds = 0 THEN CAST(@talkingHours AS VARCHAR) + ' h 0 m 0 s talking' 
					 WHEN @talkingHours != 0 AND @talkingMinutes != 0 AND @talkingSeconds = 0 THEN CAST(@talkingHours AS VARCHAR) + ' h ' + CAST(@talkingMinutes AS VARCHAR) + ' m talking' 
					 WHEN @talkingHours != 0 AND @talkingMinutes != 0 AND @talkingSeconds != 0 THEN CAST(@talkingHours AS VARCHAR) + ' h ' + CAST(@talkingMinutes AS VARCHAR) + ' m ' + CAST(@talkingSeconds AS VARCHAR) + ' s talking' 
					 WHEN @talkingHours = 0 AND @talkingMinutes != 0 AND @talkingSeconds != 0 THEN CAST(@talkingMinutes AS VARCHAR) + ' m ' + CAST(@talkingSeconds AS VARCHAR) + ' s talking' 
					 WHEN @talkingHours = 0 AND @talkingMinutes = 0 AND @talkingSeconds != 0 THEN CAST(@talkingSeconds AS VARCHAR) + ' s talking' 
				END +
				')' + 
				CHAR(13) 

			)
		END

		SET @rowCounter += 1
	END

	SET @explanationString += 'Call ended at ' + FORMAT((SELECT MAX(S_EndTime) FROM @table),'dd/MM/yyyy hh:mm:ss') 

	PRINT @explanationString
END
