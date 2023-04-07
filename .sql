USE [HUDA]
GO
/****** Object:  StoredProcedure [KIE].[sp_geocodingGoogle]    Script Date: 07.04.2023 11:51:09 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Kamil Główka
-- Create date: 2021-05-26
-- Version    : 1
-- Description:	procedura geokodująca adresy dostaw (z eKierowcy). Koordynaty pobierane są poprzez API Google. Procedura wykonywana w jobie dobowym
-- Cel: poprawne zgeokodowanie adresów dostaw z SL. Docelowo wpływa to na poprawne mierzenie odleglosci między koordynatami z apki eKiero a koordynatami klienta na dostawie
-- Uwaga: jeżeli geokodowanie dla danego adresu się nie powiedzie (zupełnie błędny adres) zostanie dla niego zwrócony NULL w tabeli LIB.eKierowcaGeocoding. Raz na jakiś czas 
-- można spróbowac geokodować wszystkie NULLe ponownie z tabelki LIB (np. ktoś poprawił adres i zaczyta koordynaty poprawnie), choć procedura będzie chodziła w jobie -30 dni 
-- więc jest to czas na poprawę adresów przez userów. W przypadku gdy chcemy geokodować NULLe z LIB najlepiej usunać je z LIB poprzez DELETE i a następnie wykonać tą procedurę 
-- (wtedy na pewno NULLe będą zwrócone do API w celu geokodowania).
-- EXEC [KIE].[sp_geocodingGoogle]
-- =============================================

ALTER PROCEDURE [KIE].[sp_geocodingGoogle]

AS
BEGIN
SET NOCOUNT ON;

-- należy jawnie ustawić textsize bo w jobie z jakiś powodów ucina xmla zwróconego w wyniku
SET TEXTSIZE 2147483647

	DECLARE 
	@Joined_ID varchar(MAX),
	@Miasto nvarchar(60),
	@ZipCode varchar(10),
	@Ulica nvarchar(60),
	@Latitude numeric(18,9),
	@Longitude numeric(18,9),
	@dataOd DATE,
	@dataDo DATE

	CREATE TABLE #tempTbl (
	Joined_ID varchar(MAX), 
	Miasto nvarchar(60), 
	Ulica nvarchar(60), 
	ZipCode nvarchar(10), 
	Latitude numeric(18,9), 
	Longitude numeric(18,9))

	SET @dataOd = DATEADD(DAY,-14,GETDATE()) --sprawdzamy przesylki za ostatnie 14 dni, czy nie nastapila aktualizacja/zmiana adresow w SL z tego okresu
	SET @dataDo = GETDATE()

--------------------------------------------------------------

	-- TABELA # pod CURSOR z adresami dostaw (na bazie danych z apki eKierowca, adresy sprawdzane za ostatnie 30 dni) 
	-- 1. czyszczenie ulic i zwracanie unikalnych contractorID
	SELECT  --TOP 200000
		c1.ContractorID,
		[KIE].CleanStreet(c1.City, 'google') as Miasto,
		[KIE].CleanStreet(c1.Street, 'google') as Ulica,
		c1.ZipCode as Kod_pocztowy

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
		CONVERT(DATE,t.ReceiptDate) BETWEEN @dataOd AND @dataDo

	GROUP BY c1.ContractorID, c1.City, c1.Street, c1.ZipCode
	--ORDER BY c1.ZipCode -- ten order by potrzebny do geokodowania po raz pierwszy, potem mozna usunac jak bedzie zapytanie chodzilo w jobie dobowym


	-- 2. sprawdzenie czy w całęj tabeli LIB istnieją już zgeokodowane adresy (ten sam ContractorID), dodatkowo sprawdzenie czy nie zmienił się adres dla danego ContractorID (jak się zmienił to zwracamy '1')
	SELECT
		COALESCE(t.ContractorID, e.ContractorID, NULL) as ContractorID, 
		COALESCE(t.Miasto, e.City, NULL) as Miasto, 
		COALESCE(t.Ulica, e.Street, NULL) as Ulica, 
		COALESCE(t.Kod_pocztowy, e.ZipCode, NULL) as Kod_pocztowy, 
		e.ContractorID as LibTbl,
		CASE WHEN t.ContractorID=e.ContractorID AND (t.Miasto != e.City OR t.Ulica != e.Street OR t.Kod_pocztowy != e.ZipCode) THEN 1 ELSE NULL END AS DoGeokodowaniaZmianaAdresu --sytuacja, gdy pojawia się nowy klient o roznym ConstractorID ale taki sam adres jak juz istniejący w tabeli LIB.. - nie będzie zatem geolokalizowany bo adres się powiela ale dodajemy go do tabeli @tempTbl bo ContractorID jest nowy - chcemy zatem i jemu przypisać lon/lat klienta z takim samym adresem. To dotyczy sytuacji gdy dodawany jest klient z takim samym adresem jakw  LIB ale innym ContractorID 

	INTO #tbl2
	FROM #tbl t 
		FULL OUTER JOIN LIB.eKierowcaGeocoding e (nolock) on e.ContractorID=t.ContractorID -- uzywamy full join zeby zwrocic wszystkie rekordy z #tbl i LIB - dzięki temu mozemy robic dalsze porownaia adresow dla wszystkich istniejących adresow


	-- 3. INSERT do @tempTbl tych wierszy, gdzie w tabeli LIB ten adres juz ISTNIEJE ale jest inny ContractorID (te rekordy nie będą geokodowane bo już adres został zgeokodowany, jednak żeby dorzucić 
	--    ten wiersz z odrębnym ContractorID do LIB musimy go wcześniej dodac do @temTbl, potem w pkt. 7 zostaną usunięte i tak ID które juz istnieja i dodane na nowo wszystkie z tabelki @tempTbl wiec bez dubli)
	--    rekordy te znajdujemy jeżeli text jest zlaczony i w kolumnie DoGeokodowaniaNoweAdresy jest wartosc != NULL (wtedy wiadomo, ze rekordy wszystkie zlaczone rekordy nie sa zupelnie nowe tylko istnialy w tablei LIB)
	--    punkt ten nie dotyczy pierwszego insertowania adresow w ogóle tylko kazdego kolejnego, gdy pojawia sie takie same adresy (job dobowy). Adresy dla tego samego ContractorID są aktualizowane jesli sie zmienia
	
	-- wyszukujemy zlączone ContractorID (istniejące adresy w tabeli LIB) - czyli to bedą rekordy, których nie chcemy powtórnie geokodowac bo adres zostal juz zgeokodowany w tabelce LIB
	SELECT tbl5.Joined_ID, tbl5.Miasto, tbl5.Ulica, tbl5.Kod_pocztowy,
		CASE WHEN tbl5.Joined_ID like '%;%' AND tbl5.DoGeokodowaniaNoweAdresy IS NOT NULL THEN tbl5.DoGeokodowaniaNoweAdresy ELSE NULL END AS doInsertowania
	INTO #chooseExistingAddress
	FROM(
	SELECT
		STRING_AGG(CAST(t.ContractorID AS VARCHAR(MAX)),';') WITHIN GROUP (ORDER BY t.Miasto DESC) as Joined_ID,
		t.Miasto, t.Ulica, t.Kod_pocztowy, MAX(t.LibTbl) as DoGeokodowaniaNoweAdresy -- wyciagamy ContractorID dla adresu istniejacego w LIB
	FROM #tbl2 t
	GROUP BY t.Miasto, t.Ulica, t.Kod_pocztowy,t.DoGeokodowaniaZmianaAdresu) as tbl5

	-- zanim ominiemy cursor, musimy dolaczyc do zlaczonych ContractorID koordynaty adresu, ktory juz istnieje w LIB. Jest to konieczne bo do #tempTbl nalezy zainsertowac rekordy z lat i long (zeby potem po splitowaniu w kazdym wierszu byl adres z koordynatami)
	-- laczymy z tabelka LIB po pierwszym ContractorID ze stringa zlaczonego, dzieki temu pobieramy koordynaty, ktore beda takie same dla kazdego z tych ContractorID
	SELECT i.Joined_ID, i.Miasto, i.Ulica, i.Kod_pocztowy, g.Latitude, g.Longitude
	INTO #insertTempTbl
	FROM LIB.eKierowcaGeocoding g (nolock)
		INNER JOIN #chooseExistingAddress i on g.ContractorID=i.doInsertowania

	-- insertowanie rekordow do @tempTbl - dzięki temu omijamy cursor i nie musimy geokodowac tego samego adresu ponownie. Cursor natomiast doda to @tempTbl tylko nowe adresy + aktualizacje
	INSERT INTO #tempTbl
	SELECT * FROM #insertTempTbl
	
	-- 4. agregacja wszystkich ContractorID z tym samym adresem (dotyczy nowych ContractorID z takim samym adresem, których jeszcze nie ma w LIB - agregacja jest po to, żeby nie geokodować tych samych 
	--    adresów ponownie przez API tylko dołączyć ContractorID do reszty ContractorID z tym samym adresem) oraz zwrócenie tych adresów, ktore są unikalne (i nie są zgeokodowane), czyli nie ma
	--	  ich jeszcze w LIB (jest to finalna tabelka z adresami do geokodowania przez API)	
	SELECT tbl3.Joined_ID, tbl3.Miasto, tbl3.Ulica, tbl3.Kod_pocztowy
	INTO #uniqueAddresses
	FROM (
	SELECT
		STRING_AGG(CAST(t.ContractorID AS VARCHAR(MAX)),';') WITHIN GROUP (ORDER BY t.Miasto DESC) as Joined_ID,
		t.Miasto, t.Ulica, t.Kod_pocztowy, MAX(t.LibTbl) as DoGeokodowaniaNoweAdresy, t.DoGeokodowaniaZmianaAdresu -- dzieki MAX wyciagamy ContractorID przypisany do danego adresu - jesli istnieje w LIB dany adres - zwróci ID, jeśli nie - zwróci NULL czyli ten adres należy wtedy geokodować

	FROM #tbl2 t
	GROUP BY t.Miasto, t.Ulica, t.Kod_pocztowy, t.DoGeokodowaniaZmianaAdresu) as tbl3
	WHERE tbl3.DoGeokodowaniaNoweAdresy IS NULL OR tbl3.DoGeokodowaniaZmianaAdresu=1

------------------------------------------------------------------

	-- 5. CURSOR - geokodowanie i insert nowych rekordów/update istniejących do tymczasowej tabeli @tempTbl (ContractorID są w niej nadal złączone)
	DECLARE googleCursor CURSOR FOR 
	SELECT u.Joined_ID, u.Miasto, u.Ulica, u.Kod_pocztowy FROM #uniqueAddresses u (nolock)
	OPEN googleCursor
	FETCH NEXT FROM googleCursor INTO @Joined_ID, @Miasto, @Ulica, @ZipCode 
	WHILE @@FETCH_STATUS = 0

		BEGIN 

		DECLARE @URL varchar(MAX)

		SET @URL = 'https://maps.googleapis.com/maps/api/geocode/xml?address=' + 
			CASE WHEN @Miasto IS NOT NULL THEN @Miasto ELSE '' END +
			CASE WHEN @Ulica IS NOT NULL THEN ', ' + @Ulica ELSE '' END +			
			CASE WHEN @ZipCode IS NOT NULL THEN ', ' + @ZipCode ELSE '' END +
			'&components=country:PL&key=AIzaSyCgbeKVdT3wS8nUU9vNzWvbjeqZh3rEXg8'   ---- language=pl& - usunięte z url, dokładniej geokoduje bez tego parametru przy adresach z '/' np. 33/35     AIzaSyAviSwswMaTU1mv1re277ErcY58nlnp8ao

		SET @URL = REPLACE(@URL,' ','+')		

		--DECLARE @Response varchar(8000)
		DECLARE @XML xml
		DECLARE @Obj int
		DECLARE @Result int
		DECLARE @HTTPStatus int
		DECLARE @ErrorMsg varchar(MAX)
		DECLARE @ErrorMsg2 varchar(MAX)

		EXEC @Result = sp_OACreate 'MSXML2.ServerXMLHttp', @Obj OUT

		--Create a temp table to hold XML values returned. 
		--Due to the size of XML values exceed VARCHAR(8000) for a number of stores returned, it must be stored in a table (or table variable).
		 IF OBJECT_ID('tempdb..#xml') IS NOT NULL DROP TABLE #xml
		 CREATE TABLE #xml (XMLValue XML)

		BEGIN TRY
			EXEC @Result = sp_OAMethod @Obj, 'open', NULL, 'GET', @URL, false
			EXEC @Result = sp_OAMethod @Obj, 'setRequestHeader', NULL, 'Content-Type', 'application/x-www-form-urlencoded'
			EXEC @Result = sp_OAMethod @Obj, send, NULL, ''
			EXEC @Result = sp_OAGetProperty @Obj, 'status', @HTTPStatus OUT
			INSERT #xml (XMLValue)  -- Cast value to the temp table created earlier
			EXEC @Result = sp_OAGetProperty @Obj, 'responseXML.xml'   --, @Response OUT
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

		SET @XML = (SELECT XMLValue FROM #XML)       -- CAST(@Response AS XML)

		SET @Latitude = @XML.value('(/GeocodeResponse/result/geometry/location/lat) [1]', 'numeric(18,9)') 
		SET @Longitude = @XML.value('(/GeocodeResponse/result/geometry/location/lng) [1]', 'numeric(18,9)')

		-- INSERT pobranych z Google danych do tabeli tymczasowej @tempTbl, przed insertowaniem do tabeli docelowej (z pojoinowanymi ContractorID, ktore w kolejnym kroku należy splitować)
		INSERT INTO #tempTbl
		SELECT @Joined_ID, @Miasto, @Ulica, @ZipCode, @Latitude, @Longitude


	FETCH NEXT FROM googleCursor INTO @Joined_ID, @Miasto, @Ulica, @ZipCode
    END 
	CLOSE googleCursor
	DEALLOCATE googleCursor

-----------------------------------------------------------

	-- 5. utworzenie tabeli gdzie każdy ContractorID będzie znajdował się w oddzielnym wierszu - splitowanie poniżej (splitujemy, żeby potem łatwo joinować tabelę LIB.eKierowcaGoecoding z inymi tabeli SL po unikalnym ContractorID)	
	SELECT s.value as ContractorID, f.Miasto, f.Ulica, f.ZipCode, f.Latitude, f.Longitude, GETDATE() as DataModyfikacji -- jest to data utworzenia nowego adresu lub jego aktualizacji/podmiany
	INTO #finalTbl
	FROM #tempTbl f
	CROSS APPLY (SELECT value FROM STRING_SPLIT(f.Joined_ID,';')) s 


	-- 6. usunięcie z LIB.eKierowcaGeocoding tych wierszy, dla których wartości zostały zmodyfikowane (od razu porownujemy wszytskie ContractorID zwrocone z #finalTbl, bez rozrozniania czy to modyfikacja czy nie)
	--    w ich miejsce wstawimy adresy z aktualnym geokodowaniem (nowe + zaktualizowane)
	DELETE FROM LIB.eKierowcaGeocoding
	WHERE ContractorID IN (SELECT ContractorID FROM #finalTbl)


	-- 7. INSERT danych z @finalTbl do tabeli docelowej
	INSERT INTO LIB.eKierowcaGeocoding 
	SELECT * FROM #finalTbl

	
	-- 8. DROP tabeli tymaczasowej zrobionej pod cursor
	DROP TABLE #tempTbl


END



