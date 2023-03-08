DECLARE @fromDate DATE = '2021-01-01' 
DECLARE @toDate DATE = '2022-12-31'
--FORMAT(S.[DATE],'ddd M/dd/yyyy')
DECLARE @dates AS NVARCHAR(MAX) = ''
;WITH CTE_Dates
AS
(
SELECT DISTINCT S.[DATE] AS Dates
FROM schedule s
WHERE S.DATE BETWEEN @fromDate AND @toDate
)
SELECT @dates += QUOTENAME(FORMAT(Dates,'ddd M/dd/yyyy')) + ',' FROM CTE_Dates ORDER BY Dates

SET @dates = LEFT(@dates,LEN(@dates)-1)

DECLARE @command VARCHAR (MAX) = ''
SET @command = 
	'SELECT *
	FROM
	(SELECT EMP_ID, SCHDATE, [Hours]
	FROM
		(SELECT 
			EMP_ID, 
			FORMAT(S.[DATE],''ddd M/dd/yyyy'') AS [SCHDATE],  
			CASE WHEN ISNULL(SUM(S.DURATION),0) > 10 THEN 10 ELSE ISNULL(SUM(S.DURATION),0) END AS [Hours]
		FROM schedule S
		WHERE S.DATE BETWEEN ' + '''' + CAST(@fromDate AS VARCHAR) + '''' + ' AND ' + '''' + CAST(@toDate AS VARCHAR) + '''' + '
		GROUP BY S.EMP_ID, S.DATE) t
	UNION ALL
	SELECT EMP_ID, ''Total'' AS Total, SUM([Hours]) AS [Hours]
	FROM
		(SELECT 
			EMP_ID, 
			FORMAT(S.[DATE],''ddd M/dd/yyyy'') AS [SCHDATE],  
			CASE WHEN ISNULL(SUM(S.DURATION),0) > 10 THEN 10 ELSE ISNULL(SUM(S.DURATION),0) END AS [Hours]
		FROM schedule S
		WHERE S.DATE BETWEEN ' + '''' + CAST(@fromDate AS VARCHAR) + '''' + ' AND ' + '''' + CAST(@toDate AS VARCHAR) + '''' + '
		GROUP BY S.EMP_ID, S.DATE) t
	GROUP BY EMP_ID) t2

	PIVOT
	(
		SUM([Hours]) FOR [SCHDATE] IN (' + @dates + ', [Total])
	) AS p'

EXECUTE (@command)
--PRINT @command

