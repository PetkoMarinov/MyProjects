DECLARE @fromDate DATETIME = '2023-01-11',  @toDate DATETIME = '2023-01-31'

SELECT
	SUM(InboundCall + OutboundCall) AS TotalCalls,
	SUM(InboundCall) AS InboundCalls,
	SUM(OutboundCall) AS OutboundCalls,
	SUM(CASE WHEN InboundCall = 1 AND Transferred = 1 THEN 1 ELSE 0 END) AS TransferredInboundCalls,
	SUM(CASE WHEN OutboundCall = 1 AND Transferred = 1 THEN 1 ELSE 0 END) AS TransferredOutboundCalls
FROM
	(SELECT 
		t.id,
		t.InboundCall,
		t.OutboundCall,
		CASE WHEN transferred.id IS NOT NULL THEN 1 ELSE 0 END AS Transferred
	FROM
		(SELECT 
			c.id,
			SUM(CASE WHEN cpa.dn_type = 1 AND cs.id = s.StartSegment THEN 1 ELSE 0 END) AS InboundCall,
			SUM(CASE WHEN cpa.dn_type != 1 AND cs.id = s.StartSegment THEN 1 ELSE 0 END) AS OutboundCall
		FROM tblCalls c
		LEFT JOIN [dbo].tblCallSegments AS cs ON cs.call_id = c.id
		LEFT JOIN [dbo].tblCallParticipants AS cp ON cp.id = cs.src_part_id 
		LEFT JOIN [dbo].tblCallParticipants AS cp1 ON cp1.id = cs.dst_part_id  
		LEFT JOIN [dbo].tblCallParties AS cpa ON cpa.id = cp.info_id 
		LEFT JOIN [dbo].tblCallParties AS cpa1 ON cpa1.id = cp1.info_id 
		LEFT JOIN (SELECT cs1.call_id, MIN(cs1.id) AS StartSegment 
				   FROM tblCalls c
				   LEFT JOIN (SELECT cs.call_id, MIN(cs.start_time) AS start_time FROM dbo.tblCallSegments AS cs GROUP BY cs.call_id) cs ON cs.call_id = c.id
				   LEFT JOIN tblCallSegments AS cs1 ON cs1.call_id = c.id AND cs1.start_time = cs.start_time 
				   WHERE cs.start_time BETWEEN @fromDate AND @toDate
				   GROUP BY cs1.call_id) s ON s.Call_Id = c.id
		WHERE c.start_time BETWEEN @fromDate AND @toDate
		GROUP BY c.id) t

		LEFT JOIN (SELECT
						c.id
				   FROM tblCalls c 
				   LEFT JOIN [dbo].tblCallSegments AS cs ON cs.call_id = c.id
				   LEFT JOIN [dbo].tblCallParticipants AS cp ON cp.id = cs.src_part_id 
				   LEFT JOIN [dbo].tblCallParticipants AS cp1 ON cp1.id = cs.dst_part_id  
				   LEFT JOIN [dbo].tblCallParties AS cpa ON cpa.id = cp.info_id 
				   LEFT JOIN [dbo].tblCallParties AS cpa1 ON cpa1.id = cp1.info_id
				   WHERE c.start_time BETWEEN @fromDate AND @toDate
					  AND cs.type = 2
					  AND cpa1.dn_type IN (0,1)
				   GROUP BY c.id
				   HAVING COUNT([type]) > 1) transferred ON transferred.id = t.id) result



