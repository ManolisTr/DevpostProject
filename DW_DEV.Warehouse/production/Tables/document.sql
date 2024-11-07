CREATE TABLE [production].[document] (

	[DocumentLevel] smallint NULL, 
	[Title] varchar(8000) NULL, 
	[Owner] int NULL, 
	[FolderFlag] bit NULL, 
	[FileName] varchar(8000) NULL, 
	[FileExtension] varchar(8000) NULL, 
	[Revision] varchar(8000) NULL, 
	[ChangeNumber] int NULL, 
	[Status] smallint NULL, 
	[rowguid] varchar(8000) NULL, 
	[ModifiedDate] datetime2(6) NULL
);

