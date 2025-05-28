/********5.Additions and status at the time of data extract*************************/ 

SET NOCOUNT ON 

Drop Table if exists #Surg;

select  EALEntryNumber
		,TreatmentFunctionCode
		,[Treatment Specialty]
		,cast(DecidedToAdmitDate as date) as DateOnList
		,cast(TCIDate as date) as TCIDate
		,RemovalDate
		,RemovalReason
		,case
			when RemovalReason in ('2','3','4') then 1
			else 0
		end as RemovedWithoutElAdmission
		,case
			when (TCIDate is null 
				or TCIDate > cast(GETDATE() as date) )
				and RemovalReason is null then 1
			else 0
		end as CurrentlyWaiting
		,case
			when RemovalReason = '1'
					or TCIDate = cast(GETDATE() as date) then 1
			else 0
		end as RemovedWithElAdmission
		,case
			when (TCIDate is not null 
				and TCIDate <= cast(GETDATE() as date) )
				and RemovalReason is null then 1
			else 0
		end as RemainedOnTheListWithTCIInPast
		,Urgency
		,SurgicalProcedureDurn
		,SurgicalProcedure

into #Surg

from ReportingViews.EAL.tblvwEALCensus

where cast(DecidedToAdmitDate as date) >= '01/04/2016'
and [Treatment Specialty] not like 'Private%'
--and ElectiveAdmissionListType in ('11','12')    --excludes planned
and AdmissionType = 'Surgical'					  -- includes Surg only



select DateOnList
		,count(*) as TotalOfAdditions
		,TreatmentFunctionCode
		,[Treatment Specialty]
		,sum(RemovedWithoutElAdmission)	as RemovedWithoutAdmission
		,sum(RemovedWithElAdmission) as RemovedWithAdmission
		,sum(CurrentlyWaiting) as CurrentlyWaiting
		,sum(RemainedOnTheListWithTCIInPast) as RemainedOnTheListWithTCIInPast
		-- ,Urgency

FROM #Surg 

group by DateOnList
		,TreatmentFunctionCode
		,[Treatment Specialty]
		-- ,Urgency

order by DateOnList
