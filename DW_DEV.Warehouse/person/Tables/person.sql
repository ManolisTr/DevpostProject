CREATE TABLE [person].[person] (

	[BusinessEntityID] int NULL, 
	[PersonType] varchar(8000) NULL, 
	[NameStyle] bit NULL, 
	[Title] varchar(8000) NULL, 
	[FirstName] varchar(8000) NULL, 
	[MiddleName] varchar(8000) NULL, 
	[LastName] varchar(8000) NULL, 
	[Suffix] varchar(8000) NULL, 
	[EmailPromotion] int NULL, 
	[rowguid] varchar(8000) NULL, 
	[ModifiedDate] datetime2(6) NULL
);

