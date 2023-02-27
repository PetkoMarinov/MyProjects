DECLARE @FromDate VARCHAR(20) = '2022-01-01', @ToDate VARCHAR(20) ='2022-12-31'	

DECLARE @dbList TABLE (ID INT IDENTITY, [Name] VARCHAR(100))
INSERT INTO @dbList([Name]) VALUES 

	('3cx_JamesRiverAir')


DECLARE @countOfDb INT = (SELECT COUNT(*) FROM @dbList)
DECLARE @dbCounter INT = 1
--DECLARE @command VARCHAR(MAX)
--DECLARE @result VARCHAR(MAX) = ''

DECLARE @resultTable TABLE 
(
	[Database] VARCHAR(100), 
	Id INT, 
	tblCalls BIT,
	tblCallSegments BIT, 
	tblCallParticipants BIT, 
	tblCallParties BIT, 
	ContainsQ BIT,
	cpStartTime DATETIME,
	Recording_Url VARCHAR(1000)
)

DECLARE @missingId TABLE (Id INT, [Database] VARCHAR(200), RowNum INT) -- a table that keeps problem id's

DECLARE @3cx_tables TABLE (TableName VARCHAR(100), RowNum INT IDENTITY)
	INSERT INTO @3cx_tables (TableName) VALUES ('tblCalls'), ('tblCallSegments'), ('tblCallParticipants')

WHILE (@dbCounter <= @countOfDb)
BEGIN
	DECLARE @database VARCHAR(200) = (SELECT [Name] FROM @dbList WHERE Id = @dbCounter) 
	DECLARE @3cx_tableCounter INT = 1

	WHILE @3cx_tableCounter <= (SELECT MAX(RowNum) FROM @3cx_tables)
	BEGIN
		DECLARE @3cx_table VARCHAR(100) = (SELECT TableName FROM @3cx_tables WHERE RowNum = @3cx_tableCounter)
		-- populate temporary table with problem id's
		DECLARE @problemIdCommand NVARCHAR(MAX) = 

			 'SELECT id,' + '''' + @database + '''' + ' AS [Database] 
			 FROM ' + QUOTENAME(@database) + '.[dbo].tblCalls  
			 WHERE start_time BETWEEN ' + '''' + @FromDate + '''' + ' AND ' + '''' + @ToDate + '''' + '
			 UNION
			 SELECT call_id,' + '''' + @database + '''' + ' AS [Database] 
			 FROM ' + QUOTENAME(@database) + '.[dbo].tblCallSegments 
			 WHERE start_time BETWEEN ' + '''' + @FromDate + '''' + ' AND ' + '''' + @ToDate + '''' + '
			 UNION 
			 SELECT call_id,' + '''' + @database + '''' + ' AS [Database] 
			 FROM ' + QUOTENAME(@database) + '.[dbo].tblCallParticipants 
			 WHERE start_time BETWEEN ' + '''' + @FromDate + '''' + ' AND ' + '''' + @ToDate + '''' + '
			 EXCEPT
			 SELECT ' + CASE WHEN @3cx_table = 'tblCalls' THEN 'Id' ELSE 'call_id' END + ',' + '''' + @database + '''' + ' AS [Database] 
			 FROM ' + QUOTENAME(@database) + '.[dbo].' + QUOTENAME(@3cx_table) + '
			 WHERE start_time BETWEEN ' + '''' + @FromDate + '''' + ' AND ' + '''' + @ToDate + '''' 
		
		INSERT INTO @missingId (Id, [Database])
			EXEC (@problemIdCommand)

		SET @3cx_tableCounter += 1
	END
	
	SET @dbCounter += 1
END

DECLARE @tempId TABLE (Id INT, [Database] VARCHAR(200), [Rank] INT)

INSERT INTO @tempId
	SELECT Id, [Database], ROW_NUMBER() OVER (PARTITION BY [Database],Id ORDER BY [Database], Id) FROM @missingId

DELETE FROM @tempId WHERE [Rank] > 1

DELETE FROM @missingId

INSERT INTO @missingId
	SELECT Id, [Database], ROW_NUMBER() OVER (PARTITION BY [Database] ORDER BY [Database], Id) FROM @tempId

SET @dbCounter = 1

WHILE (@dbCounter <= @countOfDb)
BEGIN
	SET @database = (SELECT [Name] FROM @dbList WHERE Id = @dbCounter)
	
	DECLARE @queues TABLE (QueueName VARCHAR (150), RowNum INT IDENTITY)
	DECLARE @usedQueues VARCHAR(MAX) = 
			'SELECT DISTINCT q_displayname FROM ' + QUOTENAME(@database) + '.[dbo].tblQueueCalls ' + 'WHERE time_start BETWEEN ' + '''' + @FromDate + '''' + ' AND ' + '''' + @ToDate + ''''
	
	INSERT INTO @queues (QueueName) 
		EXEC (@usedQueues)

	DECLARE @counter INT = 1
	DECLARE @totalRows INT = (SELECT MAX(RowNum) FROM @missingId WHERE [Database] = @database)

	WHILE @counter <= @totalRows
	BEGIN

		DECLARE @id INT = (SELECT id FROM @missingId WHERE [Database] = @database AND RowNum = @counter)
		DECLARE @hasData_tblCalls BIT = 0
		DECLARE @hasData_tblCallSegments BIT = 0
		DECLARE @hasData_tblCallParticipants BIT = 0
		DECLARE @hasData_tblCallParties BIT = 0
		DECLARE @isQueueSegment BIT = 0
		DECLARE @recording_Url VARCHAR(1000) = ''
		DECLARE @cpStartTime DATETIME

		DECLARE @sql_hasData_tblCalls NVARCHAR (MAX) =
			N'IF (SELECT DISTINCT id FROM ' + QUOTENAME(@database) + '.[dbo].tblCalls WHERE id = @id) IS NOT NULL
			BEGIN
				 SET @hasData_tblCalls = 1 
			END'

		EXEC sp_executesql @sql_hasData_tblCalls, N'@id INT, @hasData_tblCalls BIT OUTPUT', @id, @hasData_tblCalls OUTPUT

		DECLARE @sql_hasData_tblCallSegments NVARCHAR (MAX) =
			N'IF (SELECT DISTINCT call_id FROM ' + QUOTENAME(@database) + '.[dbo].tblCallSegments WHERE call_id = @id) IS NOT NULL 
			BEGIN
				 SET @hasData_tblCallSegments = 1 
			END' 

		EXEC sp_executesql @sql_hasData_tblCallSegments, N'@id INT, @hasData_tblCallSegments BIT OUTPUT', @id, @hasData_tblCallSegments OUTPUT

		DECLARE @sql_hasData_tblCallParticipants NVARCHAR (MAX) =
			N'IF (SELECT DISTINCT call_id FROM ' + QUOTENAME(@database) + '.[dbo].tblCallParticipants WHERE call_id = @id) IS NOT NULL 
			BEGIN
				 SET @hasData_tblCallParticipants = 1 
			END' 

		EXEC sp_executesql @sql_hasData_tblCallParticipants, N'@id INT, @hasData_tblCallParticipants BIT OUTPUT', @id, @hasData_tblCallParticipants OUTPUT

		DECLARE @sql_hasData_tblCallParties NVARCHAR (MAX) =
			N'IF (SELECT SUM(CASE WHEN cpa.id IS NULL THEN 0 ELSE 1 END)
				  FROM ' + QUOTENAME(@database) + '.[dbo].tblCallParticipants cp
				  LEFT JOIN ' + QUOTENAME(@database) + '.[dbo].tblCallParties cpa ON cpa.id = cp.info_id
				  WHERE cp.call_id = @id) > 0
			BEGIN
				SET @hasData_tblCallParties = 1 
			END'

		EXEC sp_executesql @sql_hasData_tblCallParties, N'@id INT, @hasData_tblCallParties BIT OUTPUT', @id, @hasData_tblCallParties OUTPUT

		DECLARE @sql_recording_Url NVARCHAR(1000) = 
			N'(SELECT @recording_Url = SUBSTRING((SELECT ''____'' + recording_url 
												  FROM ' + QUOTENAME(@database) + '.[dbo].tblCallParticipants 
												  WHERE recording_url IS NOT NULL AND call_id = @id 
												  FOR XML PATH('''')), 5, 1000))'
		
		EXEC sp_executesql @sql_recording_Url, N'@id INT, @recording_Url VARCHAR(1000) OUTPUT', @id, @recording_Url OUTPUT
		---------- Need future rework
		DECLARE @sql_cpStartTime NVARCHAR (MAX) =
			N'IF @hasData_tblCallParticipants = 1 
				 SET @cpStartTime = (SELECT TOP 1 start_time FROM ' + QUOTENAME(@database) + '.[dbo].tblCallParticipants WHERE call_id = @id);'

		EXEC sp_executesql @sql_cpStartTime, N'@hasData_tblCallParticipants BIT, @id INT, @cpStartTime DATETIME OUTPUT', @hasData_tblCallParticipants, @id, @cpStartTime OUTPUT

		DECLARE @queueCount INT = (SELECT COUNT(*) FROM @queues)
		DECLARE @queueCounter INT = 1

		WHILE @queueCounter <= @queueCount
		BEGIN
			DECLARE @queueName VARCHAR(150) = (SELECT QueueName FROM @queues WHERE @queueCounter = RowNum)
		
			IF @recording_Url IS NOT NULL AND CHARINDEX(@queueName, @recording_Url, 1) > 0
			BEGIN
				SET @isQueueSegment = 1
				BREAK
			END

			SET @queueCounter += 1
		END

		INSERT INTO @resultTable ([Database], Id, tblCalls, tblCallSegments, tblCallParticipants, tblCallParties, ContainsQ, cpStartTime,Recording_Url)
			SELECT @database, @id, @hasData_tblCalls, @hasData_tblCallSegments, @hasData_tblCallParticipants, @hasData_tblCallParties, @isQueueSegment, @cpStartTime, @recording_Url

		SET @counter += 1
	END

	SET @dbCounter += 1
END


SELECT * FROM @resultTable ORDER BY [Database], Id



