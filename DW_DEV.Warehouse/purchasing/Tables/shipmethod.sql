CREATE TABLE [purchasing].[shipmethod] (

	[ShipMethodID] int NULL, 
	[Name] varchar(8000) NULL, 
	[ShipBase] decimal(19,4) NULL, 
	[ShipRate] decimal(19,4) NULL, 
	[rowguid] varchar(8000) NULL, 
	[ModifiedDate] datetime2(6) NULL
);

