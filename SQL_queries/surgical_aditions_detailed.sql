/********5.Additions and status at the time of data extract*************************/ 

SET NOCOUNT ON 

Drop Table if exists #Surg;

select  EALEntryNumber
			,ElectiveAdmissionListType
			,cast(DecidedToAdmitDate as date) as DateOnList
			,cast(TCIDate as date) as TCIDate
			,TreatmentFunctionCode
			,[Treatment Specialty]
			,RemovalReason
			,RemovalDate
			,Urgency
			,IntendedSiteCode
			,IntendedManagement
			,Procedure1
			,EALAnticipatedTheatre
			,SurgicalProcedureDurn
			,SurgicalProcedure

into #Surg

from ReportingViews.EAL.tblvwEALCensus

where cast(DecidedToAdmitDate as date) >= '01/04/2016'
and [Treatment Specialty] not like 'Private%'
--and ElectiveAdmissionListType in ('11','12')    --excludes planned
and AdmissionType = 'Surgical'					  -- includes Surg only


select x.DateOnList
		,count(*) as TotalOfAdditions
		,x.TreatmentFunctionCode
		,x.[Treatment Specialty]
		,sum(x.RemovedWithoutElAdmission)	as RemovedWithoutAdmission
		,sum(x.RemovedWithElAdmission) as RemovedWithAdmission
		,sum(x.CurrentlyWaiting) as CurrentlyWaiting
		,sum(x.RemainedOnTheListWithTCIInPast) as RemainedOnTheListWithTCIInPast

from (
select EALEntryNumber
			,TreatmentFunctionCode
			,[Treatment Specialty]
			,DateOnList
			,TCIDate
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
		

		
from #Surg

where [Treatment Specialty] not like 'Private%'

)x

group by x.DateOnList
			,x.TreatmentFunctionCode
			,x.[Treatment Specialty]

order by x.DateOnList
