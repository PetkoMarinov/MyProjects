DECLARE @id INT = 2331844
DECLARE @FromDate DATETIME = '2023-01-01', @ToDate DATETIME ='2023-01-03'	

DECLARE @searchByPeriod INT = 1
DECLARE @searchById INT = 0

IF @searchByPeriod = 1 AND @searchById = 1 
BEGIN
	SET @searchByPeriod = 0
	SET @searchById = 0
END
ELSE IF @searchByPeriod = 0 AND @searchById = 0 SET @searchById = 1;
ELSE IF @searchById = 1 SET @searchByPeriod = 0;
ELSE IF @searchByPeriod = 1 SET @searchById = 0


SET NOCOUNT ON
DECLARE @failureReason TABLE (Id INT, Description VARCHAR (1000))

INSERT INTO @failureReason (Id, Description) VALUES
	(1, 'ok (connected) to '),
	(2, 'forwarded to special groups '),
	(4, 'failed to '),
	(5, 'caller forbidden '),
	(6, 'callee forbidden '),
	(7, 'limited by license '),
	(8, 'line is busy '),
	(9, 'target not found '),
	(10, 'terminated by rule '),
	(11, 'terminated by rule? '),
	(12, 'forwarding loop to '),
	(13, 'destination busy '),
	(14, 'no answer by timeout '),
	(15, 'user terminated '),
	(16, 'no answer '),
	(17, 'not registered '),
	(19, 'not responding '),
	(20, 'redirected to '),
	(21, 'server error '),
	(22, 'busy, forwarded to '),
	(23, 'route not found '),
	(24, 'route disabled '),
	(25, 'forwarded to '),
	(26, 'target disabled '),
	(27, 'external call disabled '),
	(31, 'forward failed, no outbound rule '),
	(33, 'external call forbidden ')

DECLARE @endStatus TABLE (Id INT, Description VARCHAR(100))

INSERT INTO @endStatus (Id, Description) VALUES
	(0, 'connected to '),
	(1, 'enrolled in call with '),
	(2, 'failed to '),
	(3, 'forwarded')

DECLARE @idCollection TABLE (Id INT IDENTITY, Call_id INT)

IF @searchByPeriod = 1 INSERT INTO @idCollection SELECT c.id FROM tblCalls c WHERE c.start_time BETWEEN @FromDate AND @ToDate
ELSE INSERT INTO @idCollection SELECT @id

DECLARE @lastRow INT = (SELECT MAX(Id) FROM @idCollection)
DECLARE @counter INT = 1

DECLARE @result TABLE (Call_id INT, FailureReason VARCHAR(1000))

WHILE @counter <= @lastRow
BEGIN
	
	DECLARE @currentId INT = (SELECT Call_id FROM @idCollection WHERE Id = @counter)
	DECLARE @isAnswered INT = (SELECT is_answered FROM tblCalls c WHERE c.id = @currentId)
	
	IF @isAnswered = 0	INSERT INTO @result
	SELECT 
		c.id, SUBSTRING(
			(SELECT 
				', ' 
				+ CASE WHEN cp1.failure_reason = 1 THEN 
							CASE WHEN cp1.end_status = 3 THEN 'from ' + IIF(cpa1.caller_number_display IS NULL OR cpa1.caller_number_display = '', cpa1.caller_number, cpa1.caller_number_display) + ' with which the connection failed, the call was ' + es.Description
							ELSE es.Description + IIF(cpa1.caller_number_display IS NULL OR cpa1.caller_number_display = '', cpa1.caller_number, cpa1.caller_number_display) END
					   WHEN	cp1.failure_reason IN (5,6,7,14) THEN 
							fr.Description + IIF(cpa.caller_number IS NULL OR cpa.caller_number = '', cpa.caller_number_display, cpa.caller_number)
					   WHEN cp1.failure_reason IN (21,27) THEN fr.Description
					   ELSE fr.Description + IIF(cpa1.caller_number_display IS NULL OR cpa1.caller_number_display = '', cpa1.caller_number, cpa1.caller_number_display) 
				  END
			 FROM tblCalls c 
			 LEFT JOIN tblCallSegments AS cs ON cs.call_id = c.id
			 LEFT JOIN tblCallParticipants AS cp ON cp.id = cs.src_part_id  
			 LEFT JOIN tblCallParticipants AS cp1 ON cp1.id = cs.dst_part_id  
			 LEFT JOIN tblCallParties cpa ON cpa.id = cp.info_id
			 LEFT JOIN tblCallParties cpa1 ON cpa1.id = cp1.info_id
			 LEFT JOIN @failureReason fr ON fr.Id = cp1.failure_reason
			 LEFT JOIN @endStatus es ON es.Id = cp1.end_status
			 WHERE c.id = @currentId
			FOR XML PATH('')), 3, 1000) AS FailureReason
		FROM tblcalls c 
		WHERE c.id = @currentId

	SET @counter += 1
END

SELECT * FROM @result
