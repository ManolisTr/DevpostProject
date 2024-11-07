CREATE TABLE [purchasing].[purchaseorderdetail] (

	[PurchaseOrderID] int NULL, 
	[PurchaseOrderDetailID] int NULL, 
	[DueDate] datetime2(6) NULL, 
	[OrderQty] smallint NULL, 
	[ProductID] int NULL, 
	[UnitPrice] decimal(19,4) NULL, 
	[LineTotal] decimal(19,4) NULL, 
	[ReceivedQty] decimal(8,2) NULL, 
	[RejectedQty] decimal(8,2) NULL, 
	[StockedQty] decimal(9,2) NULL, 
	[ModifiedDate] datetime2(6) NULL
);

