DECLARE @FromDate VARCHAR(20) = '2022-01-01', @ToDate VARCHAR(20) ='2022-12-31'

DECLARE @dbList TABLE (ID INT IDENTITY, [Name] VARCHAR(100))
INSERT INTO @dbList([Name]) VALUES 
	('3cx_333Help'),
	('3cx_AAAServiceNetwork'),
	('3cx_AirGroupLlc'),
	('3cx_Arca'),
	('3cx_CallHomeWorks'),
	('3cx_CoastlineHousing'),
	('3cx_Florajet'),
	('3cx_IsaacHeating'),
	('3cx_JamesRiverAir'),
	('3cx_LenThePlumber'),
	('3cx_PacificLawnSprinklers'),
	('3cx_RobinAire'),
	('3cx_Tudi')

DECLARE @countOfDb INT = (SELECT COUNT(*) FROM @dbList)
DECLARE @counter INT = 1
DECLARE @database VARCHAR(200) 
DECLARE @command VARCHAR(MAX)
DECLARE @result VARCHAR(MAX) = ''
DECLARE @columnList VARCHAR(400) 
------------------------------------------------------SHORT VERSION-------------------------
WHILE (@counter <= @countOfDb)
BEGIN
	SET @database = (SELECT [Name] FROM @dbList WHERE Id = @counter) 
	SET @columnList = 'DISTINCT c.id,' + '''' + @database + '''' + ' AS [Database]'

	SET @command =
		'SELECT ' + @columnList +'
		FROM ' + QUOTENAME(@database) + '.[dbo].tblCalls c
		LEFT JOIN ' + QUOTENAME(@database) + '.[dbo].tblCallSegments AS cs ON cs.call_id = c.id
		LEFT JOIN ' + QUOTENAME(@database) + '.[dbo].tblCallParticipants AS cp ON cp.id = cs.src_part_id 
		LEFT JOIN ' + QUOTENAME(@database) + '.[dbo].tblCallParticipants AS cp1 ON cp1.id = cs.dst_part_id  
		LEFT JOIN ' + QUOTENAME(@database) + '.[dbo].tblCallParties AS cpa ON cpa.id = cp.info_id 
		LEFT JOIN ' + QUOTENAME(@database) + '.[dbo].tblCallParties AS cpa1 ON cpa1.id = cp1.info_id 
		LEFT JOIN ' + QUOTENAME(@database) + '.[dbo].tblExtensions e ON e.ExtensionNumber = cpa1.caller_number			
		WHERE c.start_time BETWEEN ' + '''' + @FromDate + '''' + ' AND ' + '''' + @ToDate + '''' + '
		--AND cpa.dn_type = 6
'
		
	IF @counter < @countOfDb 
	BEGIN
		SET @result += (@command + CHAR(13) + 'UNION' + CHAR(13));
	END
	ELSE 
	BEGIN
		SET @result += @command + CHAR(13) + 'ORDER BY [Database], c.id';

	END
	--if @database = '3cx_LenThePlumber'  PRINT @command;
	SET @counter += 1

END

EXEC (@result)




