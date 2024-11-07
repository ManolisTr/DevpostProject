CREATE TABLE [purchasing].[vendor] (

	[BusinessEntityID] int NULL, 
	[AccountNumber] varchar(8000) NULL, 
	[Name] varchar(8000) NULL, 
	[CreditRating] smallint NULL, 
	[PreferredVendorStatus] bit NULL, 
	[ActiveFlag] bit NULL, 
	[PurchasingWebServiceURL] varchar(8000) NULL, 
	[ModifiedDate] datetime2(6) NULL
);

