DECLARE @linkedServer VARCHAR (200) = '10.1.2.21'

-- Petko Marinov
-- TO DO: DECLARE @templateServer VARCHAR (200) = '10.10.100.142'
--		  I have no rights to link template server in case script is not started on '10.10.100.142'

USE [3cx_Tables]

DECLARE @templateTable TABLE 
(
	[Table] VARCHAR (100),
	[Column] VARCHAR (100),
	[Type] VARCHAR (100),
	[Precision] INT,
	[Scale] INT,
	[CharacterSize] INT,
	IsNullable VARCHAR(10)
)

INSERT INTO @templateTable
	SELECT 
		TABLE_NAME,
		COLUMN_NAME, 
		DATA_TYPE, 
		NUMERIC_PRECISION, 
		NUMERIC_SCALE, 
		CHARACTER_MAXIMUM_LENGTH,
		IS_NULLABLE
	FROM [3cx_Tables].INFORMATION_SCHEMA.COLUMNS
	ORDER BY TABLE_CATALOG, TABLE_NAME, COLUMN_NAME

DECLARE @clientTables TABLE
(
	TABLE_CATALOG VARCHAR (200),
	TABLE_NAME VARCHAR (100),
	COLUMN_NAME VARCHAR (100),
	DATA_TYPE VARCHAR (100),
	NUMERIC_PRECISION INT,
	NUMERIC_SCALE INT,
	CHARACTER_MAXIMUM_LENGTH INT,
	IS_NULLABLE VARCHAR(10)
)

DECLARE @tempAgainstClientDb TABLE 
(
	Client VARCHAR (200),
	[Table] VARCHAR (100),
	[Column] VARCHAR (100),
	[Type] VARCHAR (100),
	[Precision] INT,
	[Scale] INT,
	[CharacterSize] INT,
	IsNullable VARCHAR(20)
)

DECLARE @dbList TABLE ([Name] VARCHAR(200),[Description] VARCHAR(1000), Id INT IDENTITY)
	
INSERT INTO @dbList ([Name], [Description])
	EXEC sp_catalogs @linkedServer

DELETE FROM @dbList WHERE [Name] NOT LIKE '3cx_%'

DECLARE @dbCount INT = (SELECT COUNT(*) FROM @dbList)

DECLARE @counter INT = 1 

WHILE @counter <= @dbCount
BEGIN
		
	DECLARE @client VARCHAR (200) = (SELECT [name] FROM @dbList WHERE Id = @counter)

	DECLARE @fillresultTable NVARCHAR (MAX) =
		N' DECLARE @templateTable TABLE 
		(
			[Table] VARCHAR (100),
			[Column] VARCHAR (100),
			[Type] VARCHAR (100),
			[Precision] INT,
			[Scale] INT,
			[CharacterSize] INT,
			IsNullable VARCHAR(10)
		)
		  
		INSERT INTO @templateTable
			SELECT 
				TABLE_NAME,
				COLUMN_NAME, 
				DATA_TYPE, 
				NUMERIC_PRECISION, 
				NUMERIC_SCALE, 
				CHARACTER_MAXIMUM_LENGTH,
				IS_NULLABLE
			FROM [3cx_Tables].INFORMATION_SCHEMA.COLUMNS
			ORDER BY TABLE_CATALOG, TABLE_NAME, COLUMN_NAME

		DECLARE @clientTables TABLE
		(
			TABLE_CATALOG VARCHAR (200),
			TABLE_NAME VARCHAR (100),
			COLUMN_NAME VARCHAR (100),
			DATA_TYPE VARCHAR (100),
			NUMERIC_PRECISION INT,
			NUMERIC_SCALE INT,
			CHARACTER_MAXIMUM_LENGTH INT,
			IS_NULLABLE VARCHAR(10)
		)

		INSERT INTO @clientTables 
			SELECT 
				TABLE_CATALOG,
				TABLE_NAME,
				COLUMN_NAME, 
				DATA_TYPE, 
				NUMERIC_PRECISION, 
				NUMERIC_SCALE, 
				CHARACTER_MAXIMUM_LENGTH,
				IS_NULLABLE 
			FROM ' + QUOTENAME(@linkedServer) + '.' + QUOTENAME(@client) + '.INFORMATION_SCHEMA.COLUMNS 
			ORDER BY TABLE_CATALOG, TABLE_NAME, COLUMN_NAME

		DECLARE @tempAgainstClientDb TABLE 
		(
			Client VARCHAR (200),
			[Table] VARCHAR (100),
			[Column] VARCHAR (100),
			[Type] VARCHAR (100),
			[Precision] INT,
			[Scale] INT,
			[CharacterSize] INT,
			IsNullable VARCHAR(20)
		)

		INSERT INTO @tempAgainstClientDb
			SELECT ' + '''' + @client + '''' + ', *
			FROM
				(SELECT * from @templateTable
				EXCEPT
				SELECT TABLE_NAME,
					COLUMN_NAME, 
					DATA_TYPE, 
					NUMERIC_PRECISION, 
					NUMERIC_SCALE, 
					CHARACTER_MAXIMUM_LENGTH,
					IS_NULLABLE 
				FROM @clientTables) t

	SELECT * FROM @tempAgainstClientDb ORDER BY Client, [Table], [Column];'

	INSERT INTO @tempAgainstClientDb
		EXEC (@fillresultTable)

	SET @counter += 1
END

DECLARE @clientDbAgainstTemplate TABLE 
(
	Client VARCHAR (200),
	[Table] VARCHAR (100),
	[Column] VARCHAR (100),
	[Type] VARCHAR (100),
	[Precision] INT,
	[Scale] INT,
	[CharacterSize] INT,
	IsNullable VARCHAR(10)
)

DECLARE @k INT = 1   

WHILE @k <= @dbCount
BEGIN
	
	DECLARE @clientDb VARCHAR (200) = (SELECT [name] FROM @dbList WHERE Id = @k)
		
	DECLARE @sql VARCHAR(MAX) = 
		'SELECT ' + '''' + @clientDb + '''' + ' AS [Database],
			TABLE_NAME,
			COLUMN_NAME,
			DATA_TYPE,
			NUMERIC_PRECISION,
			NUMERIC_SCALE,
			CHARACTER_MAXIMUM_LENGTH,
			IS_NULLABLE
		FROM ' + QUOTENAME(@linkedServer) + '.' + QUOTENAME(@clientDb) + '.INFORMATION_SCHEMA.COLUMNS
		EXCEPT
		SELECT ' + '''' + @clientDb + '''' + 'AS [Database],
			TABLE_NAME,
			COLUMN_NAME,
			DATA_TYPE,
			NUMERIC_PRECISION,
			NUMERIC_SCALE,
			CHARACTER_MAXIMUM_LENGTH,
			IS_NULLABLE
		FROM [3cx_Tables].INFORMATION_SCHEMA.COLUMNS'

	INSERT INTO @clientDbAgainstTemplate (Client, [Table], [Column], [Type], [Precision], Scale, CharacterSize,IsNullable)
		EXEC(@sql)

	SET @k += 1
END

SELECT 
	Client, 
	[Table], 
	[Column], 
	ColumnProblem,
	CASE WHEN p.ColumnProblem = 0 AND NULLIF([Type], cType) IS NOT NULL THEN 1 ELSE 0 END AS [TypeProblem],
	CASE WHEN p.ColumnProblem = 0 AND NULLIF([Precision], cPrecision) IS NOT NULL THEN 1 ELSE 0 END AS [PrecProblem],
	CASE WHEN p.ColumnProblem = 0 AND NULLIF([Scale], cScale) IS NOT NULL THEN 1 ELSE 0 END AS [ScaleProblem],
	CASE WHEN p.ColumnProblem = 0 AND NULLIF([CharacterSize], cCharacterSize) IS NOT NULL THEN 1 ELSE 0 END AS [CharProblem],
	CASE WHEN p.ColumnProblem = 0 AND NULLIF([IsNullable], cIsNullable) IS NOT NULL THEN 1 ELSE 0 END AS [IsNullProblem]
FROM
	(SELECT 
		*,
		CASE WHEN NULLIF(t.[Column], c.[cColumn]) IS NOT NULL THEN 1 ELSE 0 END AS [ColumnProblem]
	FROM
		(SELECT *, CONCAT_WS('_',[Client], [Table], [Column]) AS [Key] 
		FROM @tempAgainstClientDb) t
		LEFT JOIN
		(SELECT 
			Client AS cClient,
			[Table] AS cTable,
			[Column] AS cColumn,
			[Type] AS cType,
			[Precision] AS cPrecision,
			[Scale] AS cScale,
			[CharacterSize] AS cCharacterSize,
			[IsNullable] AS cIsNullable,
			CONCAT_WS('_',[Client], [Table], [Column]) AS [cKey] 
		FROM @clientDbAgainstTemplate) c ON c.[cKey] = t.[Key] ) p



