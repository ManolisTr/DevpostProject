CREATE TABLE [production].[product] (

	[ProductID] int NULL, 
	[Name] varchar(8000) NULL, 
	[ProductNumber] varchar(8000) NULL, 
	[MakeFlag] bit NULL, 
	[FinishedGoodsFlag] bit NULL, 
	[Color] varchar(8000) NULL, 
	[SafetyStockLevel] smallint NULL, 
	[ReorderPoint] smallint NULL, 
	[StandardCost] decimal(19,4) NULL, 
	[ListPrice] decimal(19,4) NULL, 
	[Size] varchar(8000) NULL, 
	[SizeUnitMeasureCode] varchar(8000) NULL, 
	[WeightUnitMeasureCode] varchar(8000) NULL, 
	[Weight] decimal(8,2) NULL, 
	[DaysToManufacture] int NULL, 
	[ProductLine] varchar(8000) NULL, 
	[Class] varchar(8000) NULL, 
	[Style] varchar(8000) NULL, 
	[ProductSubcategoryID] int NULL, 
	[ProductModelID] int NULL, 
	[SellStartDate] datetime2(6) NULL, 
	[SellEndDate] datetime2(6) NULL, 
	[DiscontinuedDate] datetime2(6) NULL, 
	[rowguid] varchar(8000) NULL, 
	[ModifiedDate] datetime2(6) NULL
);

