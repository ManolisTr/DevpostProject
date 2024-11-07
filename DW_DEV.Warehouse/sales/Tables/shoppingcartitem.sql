CREATE TABLE [sales].[shoppingcartitem] (

	[ShoppingCartItemID] int NULL, 
	[ShoppingCartID] varchar(8000) NULL, 
	[Quantity] int NULL, 
	[ProductID] int NULL, 
	[DateCreated] datetime2(6) NULL, 
	[ModifiedDate] datetime2(6) NULL
);

