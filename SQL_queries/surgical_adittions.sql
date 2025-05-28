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
			,TreatmentFunctionCode
			,[Treatment Specialty]
			,EALAnticipatedTheatre
			,sum(SurgicalProcedureDurn) as ProcDurationMins
				

from ReportingViews.EAL.tblvwEALCensus

where cast(DecidedToAdmitDate as date) >= '01/04/2016'
and [Treatment Specialty] not like 'Private%'
--and ElectiveAdmissionListType in ('11','12')    --excludes planned list
and AdmissionType = 'Surgical'	

group by cast(DecidedToAdmitDate as date)
			,ElectiveAdmissionListType
			,TreatmentFunctionCode
			,[Treatment Specialty]
			,EALAnticipatedTheatre
	
order by cast(DecidedToAdmitDate as date),ElectiveAdmissionListType,EALAnticipatedTheatre