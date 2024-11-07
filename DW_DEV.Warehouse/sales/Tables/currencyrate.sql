CREATE TABLE [sales].[currencyrate] (

	[CurrencyRateID] int NULL, 
	[CurrencyRateDate] datetime2(6) NULL, 
	[FromCurrencyCode] varchar(8000) NULL, 
	[ToCurrencyCode] varchar(8000) NULL, 
	[AverageRate] decimal(19,4) NULL, 
	[EndOfDayRate] decimal(19,4) NULL, 
	[ModifiedDate] datetime2(6) NULL
);

