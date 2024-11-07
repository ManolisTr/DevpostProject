CREATE TABLE [humanresources].[pk_3] (

	[BusinessEntityID] int NULL, 
	[NationalIDNumber] varchar(8000) NULL, 
	[LoginID] varchar(8000) NULL, 
	[OrganizationLevel] smallint NULL, 
	[JobTitle] varchar(8000) NULL, 
	[BirthDate] date NULL, 
	[MaritalStatus] varchar(8000) NULL, 
	[Gender] varchar(8000) NULL, 
	[HireDate] date NULL, 
	[SalariedFlag] bit NULL, 
	[VacationHours] smallint NULL, 
	[SickLeaveHours] smallint NULL, 
	[CurrentFlag] bit NULL, 
	[rowguid] varchar(8000) NULL, 
	[ModifiedDate] datetime2(6) NULL
);

