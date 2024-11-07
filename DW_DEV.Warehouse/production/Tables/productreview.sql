CREATE TABLE [production].[productreview] (

	[ProductReviewID] int NULL, 
	[ProductID] int NULL, 
	[ReviewerName] varchar(8000) NULL, 
	[ReviewDate] datetime2(6) NULL, 
	[EmailAddress] varchar(8000) NULL, 
	[Rating] int NULL, 
	[Comments] varchar(8000) NULL, 
	[ModifiedDate] datetime2(6) NULL
);

