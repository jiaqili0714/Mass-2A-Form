use JILI

SELECT *
INTO #09pnc
FROM [JiLi].[dbo].[address_list]
where company_type='Property & Casualty'



select *
INTO #rmv
from jili.dbo.rmv_carrier_name


select * from #09pnc



IF OBJECT_ID('dbo.InsName_Stopwords','U') IS NULL
CREATE TABLE dbo.InsName_Stopwords (term NVARCHAR(50) PRIMARY KEY);

MERGE dbo.InsName_Stopwords AS tgt
USING (VALUES
 (N'INC'),(N'INCORPORATED'),(N'LLC'),(N'L.L.C'),(N'CO'),(N'COMPANY'),(N'CORP'),(N'CORPORATION'),
 (N'GROUP'),(N'HOLDINGS'),(N'MUTUAL'),(N'ASSOCIATION'),(N'ASSN'),(N'ASSOCIATES'),
 (N'INSURANCE'),(N'INS'),(N'CASUALTY'),(N'INDEMNITY'),(N'ASSURANCE'),
 (N'FIRE'),(N'MARINE'),(N'PROPERTY'),(N'P&C'),(N'PC'),
 (N'THE')
) AS src(term)
ON tgt.term = src.term
WHEN NOT MATCHED THEN INSERT(term) VALUES(src.term);

-- 1) Create the override table if it doesn't exist
IF OBJECT_ID('dbo.InsurerNameOverride','U') IS NULL
BEGIN
  CREATE TABLE dbo.InsurerNameOverride (
      rmv_original_name NVARCHAR(4000) NOT NULL,
      rmv_normalized    NVARCHAR(4000) NOT NULL PRIMARY KEY,  -- join key
      mass_company_key  NVARCHAR(200)  NULL,                  -- optional Mass PK
      mass_company_name NVARCHAR(4000) NULL,
      note              NVARCHAR(4000) NULL,
      locked            BIT NOT NULL DEFAULT(1),
      updated_at        DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
  );
END
GO



DROP FUNCTION IF EXISTS dbo.NormalizeInsName;
GO
create FUNCTION dbo.NormalizeInsName (@s NVARCHAR(4000))
RETURNS NVARCHAR(4000)
AS
BEGIN
    IF @s IS NULL RETURN NULL;

    DECLARE @x NVARCHAR(4000) = UPPER(@s);

    -- unify & to AND
    SET @x = REPLACE(@x, '&', ' AND ');

    -- strip punctuation to spaces (use CHAR() for tricky ones)
    SET @x = REPLACE(@x, '.', ' ');
    SET @x = REPLACE(@x, ',', ' ');
    SET @x = REPLACE(@x, '''', ' ');
    SET @x = REPLACE(@x, '"', ' ');
    SET @x = REPLACE(@x, '/', ' ');
    SET @x = REPLACE(@x, CHAR(92), ' ');  -- backslash \
    SET @x = REPLACE(@x, '(', ' ');
    SET @x = REPLACE(@x, ')', ' ');
    SET @x = REPLACE(@x, '[', ' ');
    SET @x = REPLACE(@x, ']', ' ');
    SET @x = REPLACE(@x, '{', ' ');
    SET @x = REPLACE(@x, '}', ' ');
    SET @x = REPLACE(@x, ':', ' ');
    SET @x = REPLACE(@x, '-', ' ');

    -- collapse spaces
    WHILE CHARINDEX('  ', @x) > 0 SET @x = REPLACE(@x, '  ', ' ');
    SET @x = LTRIM(RTRIM(@x));

    -- strip leading THE
    IF LEFT(@x,4) = 'THE ' SET @x = LTRIM(SUBSTRING(@x,5,4000));

    -- iteratively strip trailing stopwords (table: dbo.InsName_Stopwords)
    DECLARE @lastSpace INT, @lastTok NVARCHAR(100);
    WHILE LEN(@x) > 0
    BEGIN
        SET @lastSpace = LEN(@x) - CHARINDEX(' ', REVERSE(@x)) + 1; -- 0 if single token
        SET @lastTok = CASE WHEN CHARINDEX(' ', @x) = 0
                            THEN @x
                            ELSE SUBSTRING(@x, @lastSpace+1, LEN(@x)-@lastSpace)
                       END;

        IF EXISTS (SELECT 1 FROM dbo.InsName_Stopwords WHERE term = @lastTok)
        BEGIN
            SET @x = CASE WHEN CHARINDEX(' ', @x) = 0
                          THEN N''
                          ELSE RTRIM(LEFT(@x, @lastSpace-1))
                     END;
        END
        ELSE BREAK;
    END

    -- final tidy
    WHILE CHARINDEX('  ', @x) > 0 SET @x = REPLACE(@x, '  ', ' ');
    SET @x = LTRIM(RTRIM(@x));

    RETURN NULLIF(@x, N'');
END
GO

--STAGE AND NORMALIZE

SELECT DISTINCT
    carrier_name,
    dbo.NormalizeInsName(carrier_name) AS nm
INTO #rmv
FROM [CO1SQLWPV10_enterpriseservices].[enterpriseservices].[dbo].[RMV_CARRIER_NAME];


SELECT DISTINCT
    company,
    dbo.NormalizeInsName(company) AS nm
INTO #mass
FROM #09pnc;

-- exact on normalized
IF OBJECT_ID('tempdb..#match_fast') IS NOT NULL DROP TABLE #match_fast;
CREATE TABLE #match_fast (
  rmv_name  NVARCHAR(4000),
  mass_name NVARCHAR(4000),
  score     DECIMAL(5,2),
  method    VARCHAR(32)
);


--EXACT
-- exact pass
INSERT INTO #match_fast (rmv_name, mass_name, score, method)
SELECT r.carrier_name, m.company, 1.00, 'EXACT_NORM'
FROM #rmv r
JOIN #mass m ON r.nm = m.nm;

-- starts-with pass
INSERT INTO #match_fast (rmv_name, mass_name, score, method)
SELECT TOP (100000)
    r.carrier_name, m.company, 0.85, 'STARTS_WITH'
FROM #rmv r
JOIN #mass m
  ON r.nm LIKE m.nm + '%' OR m.nm LIKE r.nm + '%'
WHERE r.nm IS NOT NULL AND m.nm IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM #match_fast x WHERE x.rmv_name = r.carrier_name);



  --FUZZY
-- candidates = everything not already matched
-- One-time helper
DROP FUNCTION IF EXISTS dbo.SplitTokens;
GO
CREATE FUNCTION dbo.SplitTokens (@s NVARCHAR(4000))
RETURNS @t TABLE (value NVARCHAR(100))
AS
BEGIN
    IF @s IS NULL OR LTRIM(RTRIM(@s)) = N'' RETURN;
    -- XML-based split on spaces; filters out empties
    DECLARE @x XML = N'<r><v>' + REPLACE(@s, N' ', N'</v><v>') + N'</v></r>';
    INSERT INTO @t(value)
    SELECT v.c.value('.', 'NVARCHAR(100)')
    FROM @x.nodes('/r/v') AS v(c)
    WHERE v.c.value('.', 'NVARCHAR(100)') <> N'';
    RETURN;
END
GO

IF OBJECT_ID('tempdb..#cand') IS NOT NULL DROP TABLE #cand;

CREATE TABLE #cand (
  rmv_name  NVARCHAR(4000),
  rmv_nm    NVARCHAR(4000),
  mass_name NVARCHAR(4000),
  mass_nm   NVARCHAR(4000)
);

-- Cheap blockers to avoid cross-join explosion
INSERT INTO #cand (rmv_name, rmv_nm, mass_name, mass_nm)
SELECT r.carrier_name, r.nm, m.company, m.nm
FROM #rmv r
JOIN #mass m
  ON LEFT(r.nm,1) = LEFT(m.nm,1)
 AND ABS(LEN(r.nm) - LEN(m.nm)) <= 10
WHERE r.nm IS NOT NULL AND m.nm IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM #match_fast x WHERE x.rmv_name = r.carrier_name);

-- sanity check
SELECT TOP (5) * FROM #cand;  -- verify you SEE mass_nm here

CREATE FUNCTION dbo.Levenshtein(@s NVARCHAR(4000), @t NVARCHAR(4000))
RETURNS INT
AS
BEGIN
    IF @s IS NULL OR @t IS NULL RETURN NULL;
    DECLARE @n INT = LEN(@s), @m INT = LEN(@t);
    IF @n = 0 RETURN @m;
    IF @m = 0 RETURN @n;

    DECLARE @d TABLE(i INT, j INT, cost INT, PRIMARY KEY(i,j));
    DECLARE @i INT=0; 
    WHILE @i <= @n
    BEGIN
        INSERT INTO @d(i,j,cost) VALUES(@i,0,@i);
        SET @i+=1;
    END
    DECLARE @j INT=1;
    WHILE @j <= @m
    BEGIN
        INSERT INTO @d(i,j,cost) VALUES(0,@j,@j);
        SET @j+=1;
    END

    SET @i=1;
    WHILE @i <= @n
    BEGIN
        SET @j=1;
        WHILE @j <= @m
        BEGIN
            DECLARE @cost INT = CASE WHEN SUBSTRING(@s,@i,1)=SUBSTRING(@t,@j,1) THEN 0 ELSE 1 END;
            DECLARE @a INT = (SELECT cost FROM @d WHERE i=@i-1 AND j=@j) + 1;
            DECLARE @b INT = (SELECT cost FROM @d WHERE i=@i AND j=@j-1) + 1;
            DECLARE @c INT = (SELECT cost FROM @d WHERE i=@i-1 AND j=@j-1) + @cost;
            INSERT INTO @d(i,j,cost)
            VALUES(@i,@j, (SELECT MIN(v) FROM (VALUES(@a),(@b),(@c)) AS t(v)));
            SET @j+=1;
        END
        SET @i+=1;
    END
    RETURN (SELECT cost FROM @d WHERE i=@n AND j=@m);
END
GO


;WITH U AS (
    SELECT c.rmv_name, c.mass_name, c.rmv_nm, c.mass_nm,
           COUNT(DISTINCT t1.value) AS rmv_tokens
    FROM #cand c
    CROSS APPLY dbo.SplitTokens(c.rmv_nm) t1
    GROUP BY c.rmv_name, c.mass_name, c.rmv_nm, c.mass_nm
),
V AS (
    SELECT c.rmv_name, c.mass_name,
           COUNT(DISTINCT t2.value) AS mass_tokens
    FROM #cand c
    CROSS APPLY dbo.SplitTokens(c.mass_nm) t2
    GROUP BY c.rmv_name, c.mass_name
),
I AS (
    SELECT
        c.rmv_name,
        c.mass_name,
        COUNT(DISTINCT t1.value) AS inter_tokens
    FROM #cand c
    CROSS APPLY dbo.SplitTokens(c.rmv_nm) t1
    CROSS APPLY dbo.SplitTokens(c.mass_nm) t2
    WHERE t1.value = t2.value
    GROUP BY c.rmv_name, c.mass_name
),
SCORES AS (
    SELECT
      U.rmv_name,
      U.mass_name,
      U.rmv_tokens,
      V.mass_tokens,
      I.inter_tokens,
      CAST(I.inter_tokens AS FLOAT)
        / NULLIF(U.rmv_tokens + V.mass_tokens - I.inter_tokens, 0) AS jaccard,
      dbo.Levenshtein(U.rmv_nm, U.mass_nm) AS lev,
      NULLIF(NULLIF(LEN(U.rmv_nm),0) + NULLIF(LEN(U.mass_nm),0),0) AS len_sum
    FROM U
    JOIN V ON V.rmv_name = U.rmv_name AND V.mass_name = U.mass_name
    JOIN I ON I.rmv_name = U.rmv_name AND I.mass_name = U.mass_name
)
SELECT TOP (100000)
    s.rmv_name,
    s.mass_name,
    (ISNULL(s.jaccard,0)*0.7)
    + ((1.0 - (CAST(s.lev AS FLOAT)/NULLIF(s.len_sum/2.0,0)))*0.3) AS score,
    'FUZZY' AS method
INTO #match_fuzzy
FROM SCORES s
WHERE s.rmv_tokens IS NOT NULL AND s.mass_tokens IS NOT NULL
ORDER BY score DESC;


;WITH Ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY rmv_name ORDER BY score DESC, mass_name) AS rn
  FROM (
    SELECT * FROM #match_fast
    UNION ALL
    SELECT * FROM #match_fuzzy
  ) z
)
SELECT rmv_name, mass_name, score, method
INTO #best
FROM Ranked
WHERE rn=1;



-- bring normalized RMV to join with override
IF OBJECT_ID('tempdb..#rmv_latest') IS NOT NULL DROP TABLE #rmv_latest;
SELECT DISTINCT carrier_name, dbo.NormalizeInsName(carrier_name) AS rmv_nm
INTO #rmv_latest
FROM [CO1SQLWPV10_enterpriseservices].[enterpriseservices].[dbo].[RMV_CARRIER_NAME];

-- Merge layers: overrides first, then best auto match
WITH O AS (
  SELECT r.carrier_name AS rmv_name, o.mass_company_name AS mass_name, 1.0 AS score, 'OVERRIDE' AS method
  FROM #rmv_latest r
  JOIN dbo.InsurerNameOverride o ON o.rmv_normalized = r.rmv_nm AND o.locked=1
),
A AS (
  SELECT b.rmv_name, b.mass_name, b.score, b.method
  FROM #best b
  WHERE b.score >= 0.78  -- <- threshold to tune
    AND NOT EXISTS (SELECT 1 FROM O WHERE O.rmv_name=b.rmv_name)
)
SELECT *
INTO #matches_final
FROM (
  SELECT * FROM O
  UNION ALL
  SELECT * FROM A
) q;

-- Now join to Mass Gov table to pull address/phone (using mass_name -> #09pnc.company)
SELECT
    f.rmv_name,
    p.company           AS mass_company,
    p.address,          -- adjust to your columns
    p.phone,
    P.state,-- adjust
    f.score,
    f.method
FROM #matches_final f
LEFT JOIN #09pnc p
  ON dbo.NormalizeInsName(p.company) = dbo.NormalizeInsName(f.mass_name);


--fuzzy join needs review

SELECT b.*
FROM #best b
WHERE NOT EXISTS (SELECT 1 FROM #matches_final x WHERE x.rmv_name=b.rmv_name)
ORDER BY b.rmv_name asc;

