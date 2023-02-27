SET NOCOUNT ON
DECLARE @fromDate DATETIME = '2023-01-01', @toDate DATETIME = '2023-01-09'

DECLARE	@fromHour INT = 8, @toHour INT = 17

DECLARE @agentNum NVARCHAR(50) = '109' 

DECLARE @dateCounter DATETIME = @fromDate
DECLARE @workingDays TABLE (Id INT IDENTITY, [Date] DATETIME, IsWorkDay BIT)

WHILE @dateCounter < @toDate
BEGIN
    IF DATENAME(WEEKDAY, @dateCounter) IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday')
        BEGIN
			INSERT INTO @workingDays ([Date], IsWorkDay) VALUES
			(@dateCounter, 1)
		END
    SET @dateCounter = DATEADD(DAY, 1, @dateCounter)
END

DECLARE @totalWorkDays INT = (SELECT COUNT(*) FROM @workingDays)
DECLARE @workdayStart DATETIME= DATEADD(HOUR, @fromHour, @fromDate)
DECLARE @workdayEnd DATETIME = DATEADD(HOUR, @toHour, @fromDate)
DECLARE @officeTimeInMinutes INT = DATEDIFF(MINUTE, @workdayStart, @workdayEnd)
DECLARE @lunchBreakInMinutes INT = 60
DECLARE @workTimeInSeconds INT = (@officeTimeInMinutes - @lunchBreakInMinutes) * 60

DECLARE @command VARCHAR(MAX)
DECLARE @result VARCHAR(MAX) = ''

DECLARE @counter INT = 0

WHILE (@counter < @totalWorkDays)
BEGIN
	DECLARE @currenDate DATETIME = (SELECT [Date] FROM @workingDays WHERE Id = @counter + 1)
	SET @workdayStart = DATEADD(HOUR, @fromHour, @currenDate)
	SET @workdayEnd = DATEADD(HOUR, @toHour, @currenDate)
	 
	SET @command =
		'SELECT 
			ISNULL(SUM(CASE WHEN Callee = ' + '''' + @agentNum + '''' + ' THEN Talking ELSE 0 END				    -- AS TimeBusyAsCallee,
			+												
			CASE WHEN Caller = ' + '''' + @agentNum + '''' + ' THEN Ringing + Talking ELSE 0 END), 0) AS Occupancy  -- AS TimeBusyAsCaller 
		FROM
			(SELECT 	
				CASE WHEN cp1.answer_time IS NULL OR cp1.answer_time > cp1.end_time THEN DATEDIFF(second, cs.start_time, cs.end_time) ELSE 0 END AS Ringing,
				CASE WHEN cp1.answer_time IS NOT NULL AND cpa1.dn_type <> 4 THEN
					CASE WHEN cp1.answer_time < cp1.start_time THEN DATEDIFF(second, cp1.start_time, cp1.end_time)
						 WHEN cp1.answer_time <= cp1.end_time AND cp1.end_time != cs.end_time THEN DATEDIFF(second, cs.start_time, cs.end_time) --45199 3cx_AirGroupLlc segment 189549
						 WHEN cp1.answer_time <= cp1.end_time THEN DATEDIFF(second, cp1.answer_time, cp1.end_time) 
						 ELSE 0
					END
				END AS Talking,
				cpa.caller_number AS Caller,
				cpa1.caller_number AS Callee
			FROM tblCalls c
			LEFT JOIN tblCallSegments cs ON cs.call_id = c.id
			LEFT JOIN tblCallParticipants cp ON cp.id = cs.src_part_id
			LEFT JOIN tblCallParticipants cp1 ON cp1.id = cs.dst_part_id
			LEFT JOIN tblCallParties cpa ON cpa.id = cp.info_id
			LEFT JOIN tblCallParties cpa1 ON cpa1.id = cp1.info_id
			WHERE c.start_time BETWEEN ' + '''' + CAST(@workdayStart AS VARCHAR) + '''' + ' AND ' + '''' + CAST(@workdayEnd AS VARCHAR) + '''' + '
				AND (cpa.caller_number = ' + '''' + @agentNum + '''' + ' OR cpa1.caller_number = ' + '''' + @agentNum + '''' + ')
		) m'
		
	IF @counter < @totalWorkDays - 1 
	BEGIN
		SET @result += (@command + CHAR(13) + 'UNION' + CHAR(13));
	END
	ELSE 
	BEGIN
		SET @result += @command + CHAR(13);
	END

	IF @counter = @totalWorkDays - 1
	BEGIN
		SET @result = 'SELECT FORMAT(CAST(SUM(Occupancy) AS DECIMAL) / ' + CAST(@workTimeInSeconds * @totalWorkDays AS VARCHAR) + ', ''P2'') AS OccupancyRate
	FROM
		(' + @result + ' ) t'
	END

	SET @counter += 1
END

EXEC (@result)



	
