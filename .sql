USE [HUDA]
GO
/****** Object:  StoredProcedure [KIE].[sp_geocodingGoogle]    Script Date: 07.04.2023 11:51:09 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Version    : 1.1
-- Description:	procedure goecodes customer addresses using Google Geocoding API. Procedure works in a daily job.
-- Goal: the main goal is to properly geocode all customer addresses from local TMS. For various reasons, majority of these addresses has incorrect or inaccurate coordinates.
-- Once geocoded, addresses with new, proper coordinates will be used in all geospatial analyses and applications.
-- EXEC [KIE].[sp_geocodingGoogle]
-- =============================================

ALTER PROCEDURE [KIE].[sp_geocodingGoogle]

AS
BEGIN
SET NOCOUNT ON;

SET TEXTSIZE 2147483647

	DECLARE 
	@Joined_ID varchar(MAX),
	@City nvarchar(60),
	@ZipCode varchar(10),
	@Street nvarchar(60),
	@Latitude numeric(18,9),
	@Longitude numeric(18,9),
	@dateFrom DATE,
	@dateTo DATE

	CREATE TABLE #tempTbl (
	Joined_ID varchar(MAX), 
	City nvarchar(60), 
	Street nvarchar(60), 
	ZipCode nvarchar(10), 
	Latitude numeric(18,9), 
	Longitude numeric(18,9))

	SET @dateFrom = DATEADD(DAY,-14,GETDATE())
	SET @dateTo = GETDATE()

--------------------------------------------------------------

	-- 1. Temp table (for later CURSOR) with delivery addresses.
	-- Clean addresses using CleanStreet() function and return unique ContractorID
	SELECT  
		c1.ContractorID,
		[KIE].CleanStreet(c1.City, 'google') as City,
		[KIE].CleanStreet(c1.Street, 'google') as Street,
		c1.ZipCode as ZipCode
	INTO #tbl
	FROM SKY.[Order] o (nolock)
		INNER JOIN SKY.Stage s (nolock) on o.OrderID=s.OrderID
		INNER JOIN SKY.Transit t (nolock) on s.TransitID=t.TransitID 
		LEFT JOIN KIE.Orders ed (nolock) on o.OrderNR=ed.OrderNR and ed.StageID=s.StageID
		INNER JOIN SKY.Contractor c1 (nolock) on o.ConsigneeID=c1.ContractorID

	WHERE 
		o.orderID > 5500000 AND
		s.StageTypeID=20 AND 
		o.OrderTypeID in(41,51) AND
		t.TransitTypeID=12 AND
		t.AccomplishmentID NOT IN (-10,60) AND
		CONVERT(DATE,t.ReceiptDate) BETWEEN @dateFrom AND @dateTo

	GROUP BY c1.ContractorID, c1.City, c1.Street, c1.ZipCode


	-- 2. Check if there are already geocoded addresses in the result table based on ContractorID (if so we don't want to geocode them again),
	-- Additionally, check if any address for given ContractorID has changed
	SELECT
		COALESCE(t.ContractorID, e.ContractorID, NULL) as ContractorID, 
		COALESCE(t.City, e.City, NULL) as City, 
		COALESCE(t.Street, e.Street, NULL) as Street, 
		COALESCE(t.ZipCode, e.ZipCode, NULL) as ZipCode, 
		e.ContractorID as LibTbl,
		CASE WHEN t.ContractorID=e.ContractorID AND (t.City != e.City OR t.Street != e.Street OR t.ZipCode != e.ZipCode) THEN 1 ELSE NULL END AS ChangedAddressesToGeocode
	INTO #tbl2	
	FROM #tbl t 
		FULL OUTER JOIN LIB.eDriverGeocoding e (nolock) on e.ContractorID=t.ContractorID


	-- 3. Logic for INSERT: add to @tempTbl only those addresses for which given addrees exists in LIB.eDriverGeocoding but ContractorID is different. It's related to
	--    TMS logic which allows you to add same address but with different ContractorID
	SELECT tbl5.Joined_ID, tbl5.City, tbl5.Street, tbl5.ZipCode,
		CASE WHEN tbl5.Joined_ID like '%;%' AND tbl5.NewAddressesToGeocode IS NOT NULL THEN tbl5.NewAddressesToGeocode ELSE NULL END AS ToInsert		
	INTO #chooseExistingAddress
	
	FROM(
	
	SELECT
		STRING_AGG(CAST(t.ContractorID AS VARCHAR(MAX)),';') WITHIN GROUP (ORDER BY t.City DESC) as Joined_ID,
		t.City, t.Street, t.ZipCode, MAX(t.LibTbl) as NewAddressesToGeocode		
	FROM #tbl2 t	
	GROUP BY t.City, t.Street, t.ZipCode, t.ChangedAddressesToGeocode) as tbl5


	SELECT i.Joined_ID, i.City, i.Street, i.ZipCode, g.Latitude, g.Longitude	
	INTO #insertTempTbl	
	FROM LIB.eDriverGeocoding g (nolock)
		INNER JOIN #chooseExistingAddress i on g.ContractorID=i.ToInsert

	-- INSERT addresses to @tempTbl
	INSERT INTO #tempTbl
	SELECT * FROM #insertTempTbl
	
	-- 4. Aggregate all addresses with the same ContractorID (refers to all new addresses which don't exist in LIB table yet) 	
	SELECT tbl3.Joined_ID, tbl3.City, tbl3.Street, tbl3.ZipCode
	INTO #uniqueAddresses
	FROM (
	SELECT
		STRING_AGG(CAST(t.ContractorID AS VARCHAR(MAX)),';') WITHIN GROUP (ORDER BY t.City DESC) as Joined_ID,
		t.City, t.Street, t.ZipCode, MAX(t.LibTbl) as NewAddressesToGeocode, t.ChangedAddressesToGeocode
	FROM #tbl2 t
	GROUP BY t.City, t.Street, t.ZipCode, t.ChangedAddressesToGeocode) as tbl3
	WHERE tbl3.NewAddressesToGeocode IS NULL OR tbl3.ChangedAddressesToGeocode=1

------------------------------------------------------------------

	-- 5. CURSOR - geocode and insert new records to #tempTbl
	DECLARE googleCursor CURSOR FOR 
	SELECT u.Joined_ID, u.City, u.Street, u.ZipCode FROM #uniqueAddresses u
	OPEN googleCursor
	FETCH NEXT FROM googleCursor INTO @Joined_ID, @City, @Street, @ZipCode 
	WHILE @@FETCH_STATUS = 0

		BEGIN 

		DECLARE @URL varchar(MAX)

		SET @URL = 'https://maps.googleapis.com/maps/api/geocode/xml?address=' + 
			CASE WHEN @City IS NOT NULL THEN @City ELSE '' END +
			CASE WHEN @Street IS NOT NULL THEN ', ' + @Street ELSE '' END +			
			CASE WHEN @ZipCode IS NOT NULL THEN ', ' + @ZipCode ELSE '' END +
			'&components=country:PL&key=API_KEY'

		SET @URL = REPLACE(@URL,' ','+')		

		DECLARE @XML xml
		DECLARE @Obj int
		DECLARE @Result int
		DECLARE @HTTPStatus int
		DECLARE @ErrorMsg varchar(MAX)
		DECLARE @ErrorMsg2 varchar(MAX)

		EXEC @Result = sp_OACreate 'MSXML2.ServerXMLHttp', @Obj OUT

		-- Create a temp table to hold XML values 
		 IF OBJECT_ID('tempdb..#xml') IS NOT NULL DROP TABLE #xml
		 CREATE TABLE #xml (XMLValue XML)

		BEGIN TRY
			EXEC @Result = sp_OAMethod @Obj, 'open', NULL, 'GET', @URL, false
			EXEC @Result = sp_OAMethod @Obj, 'setRequestHeader', NULL, 'Content-Type', 'application/x-www-form-urlencoded'
			EXEC @Result = sp_OAMethod @Obj, send, NULL, ''
			EXEC @Result = sp_OAGetProperty @Obj, 'status', @HTTPStatus OUT
			INSERT #xml (XMLValue)
			EXEC @Result = sp_OAGetProperty @Obj, 'responseXML.xml'
		END TRY
		
		BEGIN CATCH
			SET @ErrorMsg = ERROR_MESSAGE()
		END CATCH

		EXEC @Result = sp_OADestroy @Obj

		IF (@ErrorMsg IS NOT NULL) OR (@HTTPStatus <> 200)
		BEGIN
			SET @ErrorMsg = 'Error in spGeocode: ' + ISNULL(@ErrorMsg, 'HTTP result is: ' + CAST(@HTTPStatus as varchar(10)))
			SET @ErrorMsg2 = 'Error URL: ' + (SELECT @URL)
			RAISERROR(@ErrorMsg, 16, 1, @HTTPStatus)
			RAISERROR(@ErrorMsg2, 16, 1)
			RETURN
		END

		SET @XML = (SELECT XMLValue FROM #XML)

		SET @Latitude = @XML.value('(/GeocodeResponse/result/geometry/location/lat) [1]', 'numeric(18,9)') 
		SET @Longitude = @XML.value('(/GeocodeResponse/result/geometry/location/lng) [1]', 'numeric(18,9)')

		-- INSERT geocoded addresses to #tempTbl, before inserting to the result table
		INSERT INTO #tempTbl
		SELECT @Joined_ID, @City, @Street, @ZipCode, @Latitude, @Longitude


	FETCH NEXT FROM googleCursor INTO @Joined_ID, @City, @Street, @ZipCode
    END 
	CLOSE googleCursor
	DEALLOCATE googleCursor

-----------------------------------------------------------

	-- 6. Create a table with addresses splitted (as to make joining easier)	
	SELECT s.value as ContractorID, f.City, f.Street, f.ZipCode, f.Latitude, f.Longitude, GETDATE() as ModificationDate
	INTO #finalTbl
	FROM #tempTbl f
	CROSS APPLY (SELECT value FROM STRING_SPLIT(f.Joined_ID,';')) s 


	-- 7. Remove updated rows
	DELETE FROM LIB.eDriverGeocoding
	WHERE ContractorID IN (SELECT ContractorID FROM #finalTbl)


	-- 7. INSERT addresses from @finalTbl to result table
	INSERT INTO LIB.eDriverGeocoding 
	SELECT * FROM #finalTbl

	
	-- 8. DROP temp table
	DROP TABLE #tempTbl


END



