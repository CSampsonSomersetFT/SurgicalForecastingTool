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
			,e.TreatmentFunctionCode
			,e.[Treatment Specialty]
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
			,e.TreatmentFunctionCode
			,e.[Treatment Specialty]
			,e.EALAnticipatedTheatre
	
order by cast(RemovalDate as date),ElectiveAdmissionListType,EALAnticipatedTheatre