USE [EDW]
GO

/****** Object:  StoredProcedure [dbo].[spc_DIM_PRESCRIBER_AMA_OUTPUT_FLAG_UPPDATE]    Script Date: 4/6/2021 2:44:37 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


--spc_DIM_PRESCRIBER_AMA_OUTPUT_FLAG_UPPDATE
--spc_EDW_FACT_CASE_MGMT_CASE_LOAD
--spc_FACT_CASE_MGMT_PARTITION_RANGE_BUILD
--spc_FACT_CASE_MGMT_PARTITION_LOAD
--spc_EDW_FACT_CASE_MGMT_NOTE_VBD
--spc_EDW_FACT_CASE_MGMT_CASE_VBD
--spc_EDW_FACT_CASE_MGMT_CASE_WORKFLOW_VBD
--spc_EDW_FACT_CASE_MGMT_VBD
--spc_EDWWORK_WRK_FACT_CASE_MGMT_SF_ALLOC_LOCATION_BUILD
--spc_EDW_FACT_SURVEY_LOAD
--spc_EDW_FACT_SURVEY_COUPON_LOAD
--spc_EDW_FACT_SURVEY_LOAD_TCPLP
--spc_WRK_FACT_CASE_MGMT_CASE_ENROLLMENT_EXT_UPDATE
--spc_INDEX_REBUILD_FOR_FRAGMENTATION_STALE_STATS
--spc_MV_RPT_CASE_MGMT_PARTITION_LOAD
--spc_MV_CREATE_FOR_MSTR_REPORTING
--spc_FACT_CASE_MGMT_NONCLUSTERED_INDEX_DISABLE_REBUILD
--Dropping OLD CM SPs

-----------------------------Start spc_DIM_PRESCRIBER_AMA_OUTPUT_FLAG_UPPDATE -----------------------------

CREATE PROCEDURE [dbo].[spc_DIM_PRESCRIBER_AMA_OUTPUT_FLAG_UPPDATE]	 
AS

-- =============================================================================================
-- Author:		Vivek Palanisamy
-- Create date: 5/14/2020
-- Description:	Updates Edw.DIM_PRESCRIBER's AMA_OUTPUT_FLAG_KEY 
-- =============================================================================================

BEGIN	
	
	SET NOCOUNT ON;

	if exists(select top 1 * from edwwork.wrk_dim_prescriber_ama_optout_flag_upd)
	begin
		create clustered index CIDX_WRK_PRESCRIBER_ID_LIST_KEY on edwwork.wrk_dim_prescriber_ama_optout_flag_upd( src_prescriber_id asc, dim_prescriber_list_key asc)
		-- 6 sec

		select dim_prescriber_key ,wrk.AMA_OPTOUT_FLAG_KEY, wrk.update_dtt, wrk.RUN_CONTROL_ID, row_number() over (order by dim_prescriber_key asc) as row_nbr
		into #L_TEMP_DIM_PRESCRIBER_AMA_OPTOUT_FLAG_UPD
		from EDWWORK.WRK_DIM_PRESCRIBER_AMA_OPTOUT_FLAG_UPD as wrk
		join edw.dim_prescriber dp (nolock) on dp.SRC_PRESCRIBER_ID = wrk.src_prescriber_id 
			and dp.DIM_PRESCRIBER_LIST_KEY = wrk.dim_prescriber_list_key
		-- 10 sec to load 14 mill

		create clustered index CIDX_WRK_PRESCRIBER_ID_LIST_KEY on #L_TEMP_DIM_PRESCRIBER_AMA_OPTOUT_FLAG_UPD( row_nbr asc, dim_prescriber_key asc)
		-- 6 sec
	end

	------------------------------
	declare
		@row_count integer = 0
	,	@loop integer = 0
	,	@upd_count integer
	,	@row_cnt integer
	,	@row_cnt1 integer

	if (object_id('tempdb..#L_TEMP_DIM_PRESCRIBER_AMA_OPTOUT_FLAG_UPD') ) is not null
	begin
		select @row_count = count(1) from #L_TEMP_DIM_PRESCRIBER_AMA_OPTOUT_FLAG_UPD
	end

	--print 'total rows to be updated: ' + cast(@row_count as varchar(max))

	set @row_cnt = 0
	set @row_cnt1 = 1000000

 
	while (@loop <= @row_count / 1000000 and @row_count > 0 )
	begin

		begin transaction dp1000000

			update dp 
			set
				dp.AMA_OPTOUT_FLAG_KEY = wrk.AMA_OPTOUT_FLAG_KEY
			,	dp.UPDATE_DTT = wrk.UPDATE_DTT
			,	dp.RUN_CONTROL_ID = wrk.RUN_CONTROL_ID
			from edw.dim_prescriber dp
			join #L_TEMP_DIM_PRESCRIBER_AMA_OPTOUT_FLAG_UPD wrk on wrk.DIM_PRESCRIBER_KEY = dp.DIM_PRESCRIBER_KEY
				--and dp.dim_source_key = 13
			where
				wrk.row_nbr >= @row_cnt 
			and	wrk.row_nbr < @row_cnt1

			set @upd_count = @@ROWCOUNT

			--print 'updated count :' + cast(@upd_count as varchar(max));
			--print 'range from ' + cast(@row_cnt as varchar) + 'to ' + cast(@row_cnt1 as varchar(max));

		commit transaction dp1000000

		set @row_cnt = @row_cnt + 1000000
		set @row_cnt1 = @row_cnt1 + 1000000
		set @loop = @loop + 1

	end

	if @row_count > 0
	begin
		drop index CIDX_WRK_PRESCRIBER_ID_LIST_KEY on edwwork.wrk_dim_prescriber_ama_optout_flag_upd

		truncate table EDWWORK.WRK_DIM_PRESCRIBER_AMA_OPTOUT_FLAG_UPD
	end
	-- total update took 3 mins and 4 sec

 END

GO


