SELECT RefRecDate, Urgency, COUNT(*) as num_refs
  FROM [ReportingViews].[Referrals].[vwReferrals_TEST]
  WHERE (Lead_Clinician_Title is not null or Profile_Service LIKE '%Surgery%' OR Profile_Service NOT LIKE '%Endoscopy%')
  AND TreatFuncCode='100'
  AND DATEPART(year, GETDATE()) - DATEPART(year, RefRecDate) < 5

  GROUP BY RefRecDate, Urgency
  ORDER BY RefRecDate