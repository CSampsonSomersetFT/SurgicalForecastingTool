
/***************************************************************************************************
Additions/Removals to EWL

use mph-eprprddwh1\dwhprd
ReportingViews.EAL.tblvwEALCensus

ElectiveAdmissionListType
11 - waiting list; no admission date was given at the time a decision to admit was made
12 - booked waiting list; an admission date was given at the time a decision to admit was made 
13 - planned; this is usually part of planned sequence of clinical care determined mainly on clinical criteria
****************************************************************************************************/

/********1.Surgical Additions Simple*************************/


select  cast(DecidedToAdmitDate as date) as DateOnList
			,count(*) as NoAdds
			,ElectiveAdmissionListType
			--,TreatmentFunctionCode
			--,[Treatment Specialty]
			,EALAnticipatedTheatre
			,sum(SurgicalProcedureDurn) as ProcDurationMins
				

from ReportingViews.EAL.tblvwEALCensus

where cast(DecidedToAdmitDate as date) >= '01/04/2016'
and [Treatment Specialty] not like 'Private%'
--and ElectiveAdmissionListType in ('11','12')    --excludes planned list
and AdmissionType = 'Surgical'	

group by cast(DecidedToAdmitDate as date)
			,ElectiveAdmissionListType
			--,TreatmentFunctionCode
			--,[Treatment Specialty]
			,EALAnticipatedTheatre
	
order by cast(DecidedToAdmitDate as date),ElectiveAdmissionListType,EALAnticipatedTheatre



/********2a.Surgical Removals Simple****************************
Removals are counted when RemovalDate is not null
****************************************************************/

select  cast(e.RemovalDate as date) as RemovalDate
			,count(*) as NoRemovals
			,e.ElectiveAdmissionListType
			--,e.TreatmentFunctionCode
			--,e.[Treatment Specialty]
			,e.EALAnticipatedTheatre
							
from ReportingViews.EAL.tblvwEALCensus e


where cast(e.RemovalDate as date) >= '01/04/2016'
and [Treatment Specialty] not like 'Private%'
--and ElectiveAdmissionListType in ('11','12')    --excludes planned
and e.AdmissionType = 'Surgical'	

group by cast(e.RemovalDate as date)
			,e.ElectiveAdmissionListType
			--,e.TreatmentFunctionCode
			--,e.[Treatment Specialty]
			,e.EALAnticipatedTheatre
	
order by cast(RemovalDate as date),ElectiveAdmissionListType,EALAnticipatedTheatre


/********2b.Surgical Removals Simple with RemovalReason***************
Removals are counted when RemovalDate is not null

**********************************************************************/

select  cast(e.RemovalDate as date) as RemovalDate
			,count(*) as NoRemovals
			,e.ElectiveAdmissionListType
			,case
				when e.RemovalReason in ('2','3','4') then 'RemovedWithoutElAdmission'
				else 'RemovedWithElAdmission'
			end as RemovalGroup
			,e.RemovalReason as RemovalReasonCode
			,r.Main_Description as RemovalReasonMeaning
			--,e.TreatmentFunctionCode
			--,e.[Treatment Specialty]
			,e.EALAnticipatedTheatre
							
from ReportingViews.EAL.tblvwEALCensus e

left outer join Reference_UKHD.Data_Dictionary.Elective_Admission_List_Removal_Reason_SCD r
	on e.RemovalReason	 = r.Main_Code_Text collate SQL_Latin1_General_CP1_CI_AS
	and r.Effective_To is null

where cast(e.RemovalDate as date) >= '01/04/2016'
and [Treatment Specialty] not like 'Private%'
--and ElectiveAdmissionListType in ('11','12')    --excludes planned
and e.AdmissionType = 'Surgical'	

group by cast(e.RemovalDate as date)
			,e.ElectiveAdmissionListType
			,case
				when e.RemovalReason in ('2','3','4') then 'RemovedWithoutElAdmission'
				else 'RemovedWithElAdmission' end
			,e.RemovalReason
			,r.Main_Description
			--,e.TreatmentFunctionCode
			--,e.[Treatment Specialty]
			,e.EALAnticipatedTheatre
	
order by cast(RemovalDate as date),ElectiveAdmissionListType,EALAnticipatedTheatre

/********3.EWList Detail*************************/

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


/********4.Current wait*************************/
select cast(GETDATE() as date) as ExtractDate
		,[Treatment Specialty]
		, count(*) as NoOfWaiters

from #Surg

where (RemovalReason is null
or TCIDate > cast(GETDATE() as date))
and [Treatment Specialty] not like 'Private%'

group by [Treatment Specialty]


/********5.Additions and status at the time of data extract*************************/ 

 select x.DateOnList
		,count(*) as TotalOfAdditions
		--,x.TreatmentFunctionCode
		--,x.[Treatment Specialty]
		,sum(x.RemovedWithoutElAdmission)	as RemovedWithoutAdmission
		,sum(x.RemovedWithElAdmission) as RemovedWithAdmission
		,sum(x.CurrentlyWaiting) as CurrentlyWaiting
		,sum(x.RemainedOnTheListWithTCIInPast) as RemainedOnTheListWithTCIInPast

from (
select EALEntryNumber
			--,TreatmentFunctionCode
			--,[Treatment Specialty]
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
			--,x.TreatmentFunctionCode
			--,x.[Treatment Specialty]

order by x.DateOnList

