DECLARE @fromDate DATETIME = '2022-01-01', @toDate DATETIME = '2022-12-31'
 
DECLARE @fromHour INT = 8, @toHour INT = 17  -- I think we don't need it for the calculation of this KPI
DECLARE @agentNum NVARCHAR(50) = '3832'   --3cx_PacificLawnSprinklers

SELECT 
	SUM(MetSlaCalls) AS MetSlaCalls,
	SUM(UnmetSlaCalls) AS UnmetSlaCalls
FROM
	(SELECT qc.idcallcent_queuecalls,
		CASE WHEN qc.ts_waiting + SUM(DATEDIFF(s,0,adc.ts_polling)) <= qs.SlaTime THEN 1 END AS MetSlaCalls,
		CASE WHEN qc.ts_waiting + SUM(DATEDIFF(s,0,adc.ts_polling)) > qs.SlaTime THEN 1 END AS UnmetSlaCalls
	FROM tblQueueCalls qc
	LEFT JOIN tblAgentDroppedCalls adc ON adc.q_call_history_id = qc.call_history_id AND adc.q_num = qc.q_num
	LEFT JOIN (SELECT e.ExtensionNumber, SUM(ISNULL(CAST(SlaTime AS INT), 0)) AS SlaTime
					FROM tblExtensions e
					LEFT JOIN (SELECT 
									ep.fkiddn,
									ISNULL(ep.[value], 0) AS SlaTime
								FROM tblExtensionProperties ep
								WHERE ep.[name] = 'TOO_LONG_WAIT_NOTIFY_TIME'
								) ep ON ep.fkiddn = e.DNID
					WHERE e.ExtensionTypeID = 4
					GROUP BY e.ExtensionNumber
				) qs ON qs.ExtensionNumber = qc.q_num
	WHERE qc.to_dialednum = @agentNum
		AND qc.time_start BETWEEN @fromDate AND @toDate
		AND qc.is_answered = 1
		AND qs.SlaTime > 0
	GROUP BY qc.idcallcent_queuecalls, qc.ts_waiting, qs.SlaTime) t

