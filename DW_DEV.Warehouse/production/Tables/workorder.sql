CREATE TABLE [production].[workorder] (

	[WorkOrderID] int NULL, 
	[ProductID] int NULL, 
	[OrderQty] int NULL, 
	[StockedQty] int NULL, 
	[ScrappedQty] smallint NULL, 
	[StartDate] datetime2(6) NULL, 
	[EndDate] datetime2(6) NULL, 
	[DueDate] datetime2(6) NULL, 
	[ScrapReasonID] smallint NULL, 
	[ModifiedDate] datetime2(6) NULL
);

