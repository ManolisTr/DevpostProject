CREATE TABLE [sales].[salesorderdetail] (

	[SalesOrderID] int NULL, 
	[SalesOrderDetailID] int NULL, 
	[CarrierTrackingNumber] varchar(8000) NULL, 
	[OrderQty] smallint NULL, 
	[ProductID] int NULL, 
	[SpecialOfferID] int NULL, 
	[UnitPrice] decimal(19,4) NULL, 
	[UnitPriceDiscount] decimal(19,4) NULL, 
	[LineTotal] decimal(38,6) NULL, 
	[rowguid] varchar(8000) NULL, 
	[ModifiedDate] datetime2(6) NULL
);

