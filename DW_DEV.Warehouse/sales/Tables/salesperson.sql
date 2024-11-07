CREATE TABLE [sales].[salesperson] (

	[BusinessEntityID] int NULL, 
	[TerritoryID] int NULL, 
	[SalesQuota] decimal(19,4) NULL, 
	[Bonus] decimal(19,4) NULL, 
	[CommissionPct] decimal(10,4) NULL, 
	[SalesYTD] decimal(19,4) NULL, 
	[SalesLastYear] decimal(19,4) NULL, 
	[rowguid] varchar(8000) NULL, 
	[ModifiedDate] datetime2(6) NULL
);

