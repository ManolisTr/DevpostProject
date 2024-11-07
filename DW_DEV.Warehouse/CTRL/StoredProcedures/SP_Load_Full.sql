CREATE PROC [CTRL].[SP_Load_Full]
@stg_table NVARCHAR(100),
@table_schema NVARCHAR(100),
@table_name NVARCHAR(100)
AS

BEGIN



DECLARE @create_stm NVARCHAR(MAX);
DECLARE @truncate_stm NVARCHAR(MAX);
DECLARE @insert_stm NVARCHAR(MAX);
DECLARE @destination_table NVARCHAR(MAX);
DECLARE @table_schema_correction NVARCHAR(MAX);
DECLARE @dbo NVARCHAR(3);
DECLARE @dbo_schema NVARCHAR(10);

select  @dbo = 'dbo';
select  @dbo_schema = 'dbo_schema';

SELECT @table_schema_correction = concat('select CASE WHEN @table_schema = ''', @dbo, ''' then ''', @dbo_schema, ''' else ''', @table_schema, ''' end as CorrectedSchema');

set @destination_table = concat( @table_schema, '.', @table_name);

-- Create table if not exists
SELECT @create_stm = CONCAT('IF OBJECT_ID(N''', @destination_table, ''', N''U'') IS NULL ', 'SELECT * INTO ', @destination_table, ' from [DEV_Lakehouse].[dbo].', @stg_table);

-- -- Truncate target table
select @truncate_stm = concat('DELETE FROM ', @destination_table);

-- -- Update existing records in the destination
select @insert_stm = concat('INSERT INTO ', @destination_table , ' SELECT * FROM [DEV_Lakehouse].[dbo].', @stg_table);



EXEC sp_executesql @table_schema_correction, N'@table_schema NVARCHAR(100)', @table_schema;
EXEC sp_executesql @create_stm;
EXEC sp_executesql @truncate_stm;
EXEC sp_executesql @insert_stm;


END;