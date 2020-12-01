package com.p360.npspend.claims.analytics

object Queries {

  val APPROVED_SMRY_QUERY = """
			select provider_tin_number, 
			line_of_business_code, 
			fund_type_code,
      cast(sum(total_charge_amount) as decimal(20,3)) Billed_amount,
      cast(sum(total_allowed_amount) as decimal(20,3)) Allowed_amount,
      cast(sum(total_charge_amount)/sum(total_allowed_amount) as decimal(10,2)) as Allowed_Billed_ratio,
      '{effdate}' as effective_date,
      '{expdate}' as expiry_date
      from claimssummary_green
      where claim_status_code = 'A'
      and first_service_date between (date '{npsstartdate}' - interval '12' month) and date '{npsstartdate}'
      group by provider_tin_number, line_of_business_code, fund_type_code 
			"""
}