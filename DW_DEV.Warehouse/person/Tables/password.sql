CREATE TABLE [person].[password] (

	[BusinessEntityID] int NULL, 
	[PasswordHash] varchar(8000) NULL, 
	[PasswordSalt] varchar(8000) NULL, 
	[rowguid] varchar(8000) NULL, 
	[ModifiedDate] datetime2(6) NULL
);

