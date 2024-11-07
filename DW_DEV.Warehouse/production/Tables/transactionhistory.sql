CREATE TABLE [production].[transactionhistory] (

	[TransactionID] int NULL, 
	[ProductID] int NULL, 
	[ReferenceOrderID] int NULL, 
	[ReferenceOrderLineID] int NULL, 
	[TransactionDate] datetime2(6) NULL, 
	[TransactionType] varchar(8000) NULL, 
	[Quantity] int NULL, 
	[ActualCost] decimal(19,4) NULL, 
	[ModifiedDate] datetime2(6) NULL
);

