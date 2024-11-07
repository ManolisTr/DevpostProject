CREATE TABLE [production].[workorderrouting] (

	[WorkOrderID] int NULL, 
	[ProductID] int NULL, 
	[OperationSequence] smallint NULL, 
	[LocationID] smallint NULL, 
	[ScheduledStartDate] datetime2(6) NULL, 
	[ScheduledEndDate] datetime2(6) NULL, 
	[ActualStartDate] datetime2(6) NULL, 
	[ActualEndDate] datetime2(6) NULL, 
	[ActualResourceHrs] decimal(9,4) NULL, 
	[PlannedCost] decimal(19,4) NULL, 
	[ActualCost] decimal(19,4) NULL, 
	[ModifiedDate] datetime2(6) NULL
);

