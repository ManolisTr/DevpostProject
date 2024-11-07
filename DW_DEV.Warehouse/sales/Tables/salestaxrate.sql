CREATE TABLE [sales].[salestaxrate] (

	[SalesTaxRateID] int NULL, 
	[StateProvinceID] int NULL, 
	[TaxType] smallint NULL, 
	[TaxRate] decimal(10,4) NULL, 
	[Name] varchar(8000) NULL, 
	[rowguid] varchar(8000) NULL, 
	[ModifiedDate] datetime2(6) NULL
);

