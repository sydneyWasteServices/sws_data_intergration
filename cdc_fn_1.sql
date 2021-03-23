USE SWS_CDC_TEST 
GO

EXEC sys.sp_cdc_help_change_data_capture

EXEC sp_changedbowner 'sa'

EXEC sys.sp_cdc_enable_db

EXEC sys.sp_MScdc_capture_job


EXEC sys.sp_cdc_enable_table
    @source_schema = 'TEST_1'
    , @source_name = 'TO_SQL'
    , @role_name = NULL
GO



Select * FROM msdb.dbo.sysjobs

cdc.SWS_CDC_TEST_capture
CDC Log Scan Job


EXEC SP_CONFIGURE 'cdc.SWS_CDC_TEST_capture',1

SELECT * FROM msdb.dbo.sysjobsteps


command
sys.sp_MScdc_cleanup_job

command
sys.sp_MScdc_capture_job

step_name


USE SWS_CDC_TEST 
GO


USE msdb ;  
GO  

EXEC dbo.sp_start_job 
    @job_id = '3e36f355-ebc9-4559-b7b3-da07e4c6e819'
GO  