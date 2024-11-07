CREATE TABLE [purchasing].[productvendor] (

	[ProductID] int NULL, 
	[BusinessEntityID] int NULL, 
	[AverageLeadTime] int NULL, 
	[StandardPrice] decimal(19,4) NULL, 
	[LastReceiptCost] decimal(19,4) NULL, 
	[LastReceiptDate] datetime2(6) NULL, 
	[MinOrderQty] int NULL, 
	[MaxOrderQty] int NULL, 
	[OnOrderQty] int NULL, 
	[UnitMeasureCode] varchar(8000) NULL, 
	[ModifiedDate] datetime2(6) NULL
);

