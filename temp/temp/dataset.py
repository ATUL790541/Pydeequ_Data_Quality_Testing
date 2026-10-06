DATASETS = {
    "Danone": {

        # ---------------------------
        # 🔹 FACT TABLES (Correct)
        # ---------------------------
        "planned_customer_dimension": {
            "table": "asper_production_danone_us_production_catalog.silver_us.star_planned_customer_dimension",
            "sheet_name": "Planned Customer Dimension",
            "keys": ["PLANNED_CUSTOMER_CODE"]
        },

        "pos_fact_upc": {
            "table": "asper_production_danone_us_production_catalog.silver_us.star_pos_fact_upc",
            "sheet_name": "Pos Fact (UPC)",
            "keys": ["UPC", "GEOGRAPHY_KEY", "WEEK_ENDING_DATE"]
        },

        "product_dimension": {
            "table": "asper_production_danone_us_production_catalog.silver_us.star_product_dimension",
            "sheet_name": "Material Dimension",
            "keys": []   # 👈 no duplicate check for now
        },

        "tpm_planning_fact": {
            "table": "asper_production_danone_us_production_catalog.silver_us.star_tpm_planning_fact",
            "sheet_name": "TPM Planning Fact",
            "keys": ["SALES_ORGANIZATION_CODE", "PROMOTION_ID", "PLANNING_EVENT_ID", "MATERIAL_GROUP_ID"]
        }
    },

    # ---------------------------
    # 🔹 KIND (unchanged)
    # ---------------------------
    "Kind": {
        "pos_data_upc": {
            "table": "asper_production_kind_us_production_catalog.silver_us.pos_data_upc",
            "sheet_name": "Pos Fact (UPC)",
            "keys": ["UPC", "GEOGRAPHY_KEY", "WEEK_ENDING_DATE"]
        }
    },
"Hormel": {

        "material_dimension": {
            "table": "asper_production_hormel_us_production_catalog.silver_us.star_dim_material",
            "sheet_name": "Material Dimension",
            "keys": ["UPC"]
        },

        "geo_dimension": {
            "table": "asper_production_hormel_us_production_catalog.silver_us.star_dim_geo",
            "sheet_name": "Geo Dimension",
            "keys": ["GEOGRAPHY_KEY"]
        },

        "date_day_dimension": {
            "table": "asper_production_hormel_us_production_catalog.silver_us.star_dim_date_day",
            "sheet_name": "Date Dimension",
            "keys": ["CURRENT_YEAR_CALENDAR_DATE"]
        },

        "date_week_dimension": {
            "table": "asper_production_hormel_us_production_catalog.silver_us.star_dim_date_fiscal_week",
            "sheet_name": "Date Dimension",
            "keys": ["FISCAL_YEAR_WEEK"]
        },

        "retailer_dimension": {
            "table": "asper_production_hormel_us_production_catalog.silver_us.star_dim_retailer",
            "sheet_name": "Retailer Dimension",
            "keys": ["PLANT_TO_ID"]
        },

        "product_dimension": {
            "table": "asper_production_hormel_us_production_catalog.silver_us.star_dim_product",
            "sheet_name": "Material Dimension",
            "keys": ["PPG"]
        },

        "customer_planto_dimension": {
            "table": "asper_production_hormel_us_production_catalog.silver_us.star_dim_customer_planto",
            "sheet_name": "Planned Customer Dimension",
            "keys": ["PLANNED_CUSTOMER_CODE"]
        },

        "customer_dimension": {
            "table": "asper_production_hormel_us_production_catalog.silver_us.star_dim_customer",
            "sheet_name": "Retailer Dimension",
            "keys": ["DISTRIBUTOR_CUSTOMER_CODE"]
        },

        "tpm_promotion_dimension": {
            "table": "asper_production_hormel_us_production_catalog.silver_us.star_dim_tpm_promotion",
            "sheet_name": "TPM Promotion Dimension",
            "keys": ["PROMOTION_ID", "PPG_ID"]
        },

        "tpm_plan_dimension": {
            "table": "asper_production_hormel_us_production_catalog.silver_us.star_dim_tpm_plan",
            "sheet_name": "TPM Plan Dimension",
            "keys": ["PLAN_ID"]
        }
    }
}


# 🔹 Common schema file
SCHEMA_PATH = "/Volumes/asper_production_danone_us_production_catalog/silver_us/silver_volume/ADP Silver Schema-Latest.xlsx"