CREATE TABLE [production].[billofmaterials] (

	[BillOfMaterialsID] int NULL, 
	[ProductAssemblyID] int NULL, 
	[ComponentID] int NULL, 
	[StartDate] datetime2(6) NULL, 
	[EndDate] datetime2(6) NULL, 
	[UnitMeasureCode] varchar(8000) NULL, 
	[BOMLevel] smallint NULL, 
	[PerAssemblyQty] decimal(8,2) NULL, 
	[ModifiedDate] datetime2(6) NULL
);

