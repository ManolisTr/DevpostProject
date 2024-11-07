CREATE TABLE [production].[productinventory] (

	[ProductID] int NULL, 
	[LocationID] smallint NULL, 
	[Shelf] varchar(8000) NULL, 
	[Bin] smallint NULL, 
	[Quantity] smallint NULL, 
	[rowguid] varchar(8000) NULL, 
	[ModifiedDate] datetime2(6) NULL
);

