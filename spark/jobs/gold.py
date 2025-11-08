"""Defines Spark jobs in the GOLD layer of the architecture"""
from common_spark import spark

GOLD_NAMESPACE = "exa.gold"
if __name__ == "__main__":
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {GOLD_NAMESPACE}")
    spark.sql(
        f"""
    CREATE TABLE IF NOT EXISTS {GOLD_NAMESPACE}.top_locations_by_insurance_claims AS
    SELECT
      pz.address_country AS country,
      pz.address_city AS city,
      ROUND(SUM(COALESCE(b.payment_amount.value, 0)), 2) AS total_payments
    FROM (
      SELECT
        p.id,
        z.address_country AS address_country,
        z.address_city AS address_city
      FROM exa.silver.patient p
      LATERAL VIEW explode(arrays_zip(p.address_country, p.address_city)) t AS z
    ) AS pz
    JOIN exa.silver.explanationofbenefit AS b
      ON concat('urn:uuid:', pz.id) = b.patient_reference
    GROUP BY pz.address_country, pz.address_city
      """
    ).show()

    spark.sql(
        f"""SELECT * FROM {GOLD_NAMESPACE}.top_locations_by_insurance_claims 
    ORDER BY total_payments DESC
    LIMIT 10
  """
    ).show()
