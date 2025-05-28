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