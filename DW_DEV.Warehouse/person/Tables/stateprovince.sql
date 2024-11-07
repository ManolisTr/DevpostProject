CREATE TABLE [person].[stateprovince] (

	[StateProvinceID] int NULL, 
	[StateProvinceCode] varchar(8000) NULL, 
	[CountryRegionCode] varchar(8000) NULL, 
	[IsOnlyStateProvinceFlag] bit NULL, 
	[Name] varchar(8000) NULL, 
	[TerritoryID] int NULL, 
	[rowguid] varchar(8000) NULL, 
	[ModifiedDate] datetime2(6) NULL
);

