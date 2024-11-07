CREATE TABLE [sales].[specialoffer] (

	[SpecialOfferID] int NULL, 
	[Description] varchar(8000) NULL, 
	[DiscountPct] decimal(10,4) NULL, 
	[Type] varchar(8000) NULL, 
	[Category] varchar(8000) NULL, 
	[StartDate] datetime2(6) NULL, 
	[EndDate] datetime2(6) NULL, 
	[MinQty] int NULL, 
	[MaxQty] int NULL, 
	[rowguid] varchar(8000) NULL, 
	[ModifiedDate] datetime2(6) NULL
);

