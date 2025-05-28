
/********2a.Surgical Removals Simple****************************
Removals are counted when RemovalDate is not null
****************************************************************/

select  cast(e.RemovalDate as date) as RemovalDate
			,count(*) as NoRemovals
			,e.ElectiveAdmissionListType
			,e.TreatmentFunctionCode
			,e.[Treatment Specialty]
			,e.EALAnticipatedTheatre
							
from ReportingViews.EAL.tblvwEALCensus e


where cast(e.RemovalDate as date) >= '01/04/2016'
and [Treatment Specialty] not like 'Private%'
--and ElectiveAdmissionListType in ('11','12')    --excludes planned
and e.AdmissionType = 'Surgical'	

group by cast(e.RemovalDate as date)
			,e.ElectiveAdmissionListType
			,e.TreatmentFunctionCode
			,e.[Treatment Specialty]
			,e.EALAnticipatedTheatre
	
order by cast(RemovalDate as date),ElectiveAdmissionListType,EALAnticipatedTheatre