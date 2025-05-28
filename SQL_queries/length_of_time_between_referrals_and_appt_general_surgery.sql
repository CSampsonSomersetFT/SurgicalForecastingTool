SELECT DATEDIFF(day, RefRecDate, PWLApptDate) as days_waited, Urgency
  FROM [ReportingViews].[Referrals].[vwReferrals_TEST]
  WHERE (Lead_Clinician_Title is not null or Profile_Service LIKE '%Surgery%' OR Profile_Service NOT LIKE '%Endoscopy%')
  AND TreatFuncCode='100'
  AND DATEPART(year, GETDATE()) - DATEPART(year, RefRecDate) < 5
  AND PWLApptDate is not NULL