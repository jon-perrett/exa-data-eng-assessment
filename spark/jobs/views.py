from common_spark import spark

RESOURCE_TYPES = [
    "Patient",
    "Encounter",
    "CareTeam",
    "CarePlan",
    "DiagnosticReport",
    "DocumentReference",
    "Claim",
    "ExplanationOfBenefit",
    "Coverage",
    "AllergyIntolerance",
    "Condition",
]
spark.sql("CREATE NAMESPACE IF NOT EXISTS exa.gold").show()
for resource_type in RESOURCE_TYPES:
    spark.sql(
        f"""
        CREATE VIEW IF NOT EXISTS exa.gold.v_{resource_type.lower()}s AS
        SELECT * FROM exa.silver.resources WHERE resourceType = '{resource_type}'
      """
    )
