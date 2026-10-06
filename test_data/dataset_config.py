DATASETS = {
    #  "Danone": {
    #      "planned_customer_dimension": {
    #          "table": "asper_production_danone_us_production_catalog.silver_us.star_planned_customer_dimension",
    #          "sheet_name": "Planned Customer Dimension",
    #          "keys": ["PLANNED_CUSTOMER_CODE"]
    #      },

        #  "pos_fact_upc": {
        #     "table": "asper_production_danone_us_production_catalog.silver_us.star_pos_fact_upc",
        #      "sheet_name": "Pos Fact (UPC)",
        #      #"keys":["SALES_ORGANIZATION_CODE","CHANNEL_CODE","RETAILER","UPC", "GEOGRAPHY_DESCRIPTION", "WEEK_ENDING_DATE"]
        #      "keys":["UPC","GEOGRAPHY_KEY","WEEK_ENDING_DATE"]
        # },

        #  "product_dimension": {
        #      "table": "asper_production_danone_us_production_catalog.silver_us.star_product_dimension",
        #      "sheet_name": "Material Dimension",
        #      "keys": ["UPC"]   # no duplicate check for now

        #  },

    #      "tpm_planning_fact": {
    #          "table": "asper_production_danone_us_production_catalog.silver_us.star_tpm_planning_fact",
    #          "sheet_name": "TPM Planning Fact",
    #          "keys": ["SALES_ORGANIZATION_CODE", "PROMOTION_ID", "PLANNING_EVENT_ID", "MATERIAL_GROUP_ID","WEEK_KEY"]
    #      },
    #     "retailer_dimension": {
    #         "table": "asper_production_danone_us_production_catalog.silver_us.star_retailer_dimension",
    #         "sheet_name": "Retailer Dimension",
    #         "keys": ["RETAILER", "GEOGRAPHY_KEY"]
    #      },
    #      "Geography_dimension":
    #      {
    #          "table": "asper_production_danone_us_production_catalog.silver_us.star_geo_dimension",
    #          "sheet_name": "Geo Dimension",
    #          "keys": ["GEOGRAPHY_KEY"]

    #      },
    #      "PNL_Fact":
    #      {
    #          "table": "asper_production_danone_us_production_catalog.silver_us.star_pnl_fact",
    #          "sheet_name": "PNL Fact (PPG)",
    #          "keys": ["SALES_ORGANIZATION_CODE", "CHANNEL_CODE", "RETAILER_ID",
    #             "PERIOD_START_DATE", "PPG", "METRIC_CATEGORY"]

    #      }
    #   },

    
    
     "Sauersbrands": {
         
      "geo_dim": {
        #"table": "asper_production_sauersbrands_us_catalog.silver_us.star_geo_dim",
        "table" :"asper_production_asper_us_dp_dev_catalog.test_atul.star_geo_dim",
        "sheet_name": "Geo Dimension",
        "keys": ["GEOGRAPHY_KEY", "SOURCE_SYSTEM"]
      },
      "time_dim": {
        #"table": "asper_production_sauersbrands_us_catalog.silver_us.star_time_dim",
        "table": "asper_production_asper_us_dp_dev_catalog.test_atul.star_time_dim",
        "sheet_name": "Date Dimension",
        "keys": ["DATE_KEY", "SOURCE"]
      },
      # "product_dim_pos": {
      #   "table": "asper_production_sauersbrands_us_catalog.silver_us.star_product_dim_pos",
      #   "sheet_name": "Material Dimension",
      #   "keys": ["PRODUCT_KEY", "RECORD_SOURCE"]
      # },
      # "fact_pos_kroger_combined": {
      #   "table": "asper_production_sauersbrands_us_catalog.silver_us.star_fact_pos_kroger_combined",
      #   "sheet_name": "Pos Fact (UPC)",
      #   "keys": ["GEOGRAPHY_KEY", "DATE_KEY", "NATIVE_PRODUCT_KEY", "SOURCE"]
      # },
    #   "fact_pos_kroger_combined_kroger_grain": {
    #     "table": "asper_production_sauersbrands_us_catalog.silver_us.star_fact_pos_kroger_combined",
    #     "sheet_name": "Pos Fact (UPC)",
    #     "keys": ["UPC", "WEEK_ENDING_DATE", "SOURCE", "GEOGRAPHY_DESCRIPTION"]
    #   }
    # }
         
    #     "geo_dim_sauers": {
    #         "table": "asper_production_sauersbrands_us_catalog.silver_us.star_geo_dim",
    #         "sheet_name": "Geo Dimension",
    #         "keys": ["SOURCE_SYSTEM", "GEOGRAPHY_KEY"]
    #     },
    #     "time_dim_sauers": {
    #         "table": "asper_production_sauersbrands_us_catalog.silver_us.star_time_dim",
    #         "sheet_name": "Date Dimension",
    #         "keys": ["SOURCE", "DATE_KEY"]
    #     },
    #     "product_dim_pos_sauers": {
    #         "table": "asper_production_sauersbrands_us_catalog.silver_us.star_product_dim_pos",
    #         "sheet_name": "Material Dimension",
    #         "keys": ["RECORD_SOURCE", "PRODUCT_KEY"]
    #     },
    #     "pos_fact_sauers": {
    #         "table": "asper_production_sauersbrands_us_catalog.silver_us.star_pos_fact",
    #         "sheet_name": "Pos Fact (UPC)",
    #         "keys": ["WEEK_ENDING_DATE", "GEOGRAPHY_DESCRIPTION", "UPC", "SOURCE"]
    #     },
    #     "kroger_pos_sauers": {
    #         "table": "asper_production_sauersbrands_us_catalog.silver_us.star_kroger_pos",
    #         "sheet_name": "Pos Fact (UPC)",
    #         "keys": ["WEEK_ENDING_DATE", "GEOGRAPHY_DESCRIPTION", "UPC", "SOURCE"]
    #     },
        # "fact_pos_kroger_combined_sauers": {
        #     "table": "asper_production_sauersbrands_us_catalog.silver_us.star_fact_pos_kroger_combined",
        #     "sheet_name": "Pos Fact (UPC)",
        #     "keys": ["WEEK_ENDING_DATE", "GEOGRAPHY_DESCRIPTION", "UPC", "SOURCE"]
        # }
    #     "kroger_pos_fact": {
    #         "table": "asper_production_sauersbrands_us_catalog.silver_us.star_kroger_pos",
    #         "sheet_name": "Pos Fact (UPC)",
    #         "keys": [
    #              "WEEK_ENDING_DATE",
    #             "GEOGRAPHY_DESCRIPTION",
    #             "UPC",
    #             "SOURCE"],
    #              },
    #     "fact_pos_kroger_combined": {
    #         "table": "asper_production_sauersbrands_us_catalog.silver_us.star_fact_pos_kroger_combined",
    #         "sheet_name": "Pos Fact (UPC)",
    #         "keys": [
    #             "WEEK_ENDING_DATE",
    #             "GEOGRAPHY_DESCRIPTION",
    #             "UPC",
    #             "SOURCE"],
    #             },

    #     "geo_dim": {
    #            "table": "asper_production_sauersbrands_us_catalog.silver_us.star_geo_dim",
    #            "sheet_name": "Geo Dimension",
    #            "keys": ["GEOGRAPHY_KEY","SOURCE_SYSTEM"]
    #                 },
    #     "time_dim": {
    #            "table": "asper_production_sauersbrands_us_catalog.silver_us.star_time_dim",
    #            "sheet_name": "Date Dimension",
    #            "keys": ["DATE_KEY","SOURCE"]
    #            },
    #     "product_dim_pos": {
    #            "table": "asper_production_sauersbrands_us_catalog.silver_us.star_product_dim_pos",
    #            "sheet_name": "Material Dimension",
    #            "keys": ["PRODUCT_KEY","RECORD_SOURCE"]
    #            },
        
    #     "pos_fact": {
    #            "table": "asper_production_sauersbrands_us_catalog.silver_us.star_pos_fact",
    #            "sheet_name": "Pos Fact (UPC)",
    #            #"keys": ["GEOGRAPHY_KEY","DATE_KEY","NATIVE_PRODUCT_KEY","SOURCE"]
    #            "keys": [
    #                         "WEEK_ENDING_DATE",
    #                         "GEOGRAPHY_DESCRIPTION",
    #                         "UPC",
    #                         "SOURCE"]
      }
        

#         "product_dimension": {
#         "table": "asper_production_sauersbrands_us_catalog.silver_us.star_product",
#         "sheet_name": "Material Dimension",
#         "keys": ["UPC"]
#     },
     

        
    #  },
      
    # "Perrigo_France": {
     #      "shipment_fact_france": {
     #           "table": "asper_prod_uk_perrigo_france_catlog.silver.star_shipment_fact",
     #           "sheet_name": "Shipment Fact",
     #           "keys": ["INVOICE_ID","NATIVE_PRODUCT_ID","FISCAL_YEAR_PERIOD"]
     #           },
        #    "product_dimension": {
        #        "table": "asper_prod_uk_perrigo_france_catlog.silver.star_product_dimension",
        #        "sheet_name": "Material Dimension",
        #        "keys": ["MATERIAL_CODE"]
        #        },
        #     "list_price_fact_france": {
        #             "table": "asper_prod_uk_perrigo_france_catlog.silver.star_list_price_fact",
        #             "sheet_name": "List Price",
        #             "keys": [
        #                 "SALES_ORGANIZATION_CODE",
        #                 "CONDITION_TYPE_CODE",
        #                 "CUSTOMER_ID",
        #                 "MATERIAL_ID",
        #                 "VALID_FROM_DATE",
        #                 "PRICE_LIST_CODE"
        #             ]
        #             },
     #      "customer_dimension": {
     #           "table": "asper_prod_uk_perrigo_france_catlog.silver.star_customer_dimension",
     #           "sheet_name": "Retailer Dimension",
     #           "keys": ["CUSTOMER_ID"]
     #           },
          #  "sellout_material_dimension_france": {
          #      "table": "asper_prod_uk_perrigo_france_catlog.silver.star_sellout_material_dimension",
          #      "sheet_name": "Material Dimension",
          #      "keys": ["UPC_13_DIGIT"]
          #      },
          # "sellout_channel_dimension_france": {
          #      "table": "asper_prod_uk_perrigo_france_catlog.silver.star_sellout_channel_dimension",
          #      "sheet_name": "Channel Dimension",
          #      "keys": ["CHANNEL_KEY"]
          #      },

          # "date_dim_sellout_france": {
          #      "table": "asper_prod_uk_perrigo_france_catlog.silver.star_date_dim_sellout",
          #      "sheet_name": "Date Dimension",
          #      "keys": ["DATE_KEY"]
          #      },
          # "inventory_fact_sellout_france": {
          #      "table": "asper_prod_uk_perrigo_france_catlog.silver.star_inventory_fact_sellout",
          #      "sheet_name": "Inventory Fact",
          #      "keys": ["DATE_KEY", "UPC_13_DIGIT", "CHANNEL_CODE", "EAN_NO"]
          #      },
          # "fact_pos_sellout_france": {
          #      "table": "asper_prod_uk_perrigo_france_catlog.silver.star_fact_pos_sellout",
          #      "sheet_name": "Pos Fact (UPC)",
          #      "keys": ["DATE_KEY", "UPC_13_DIGIT", "CHANNEL_CODE", "EAN_NO"]
          #      },
          # "pos_fact_distribution_sellout_france": {
          #      "table": "asper_prod_uk_perrigo_france_catlog.silver.star_pos_fact_distribution_sellout",
          #      "sheet_name": "Pos Fact (Distribution)",
          #      "keys": ["DATE_KEY", "UPC", "CHANNEL_CODE", "EAN_NO"]
          #      },
          
    #         "bom_component_fact_france": {
    #         "table": "asper_prod_uk_perrigo_france_catlog.silver.star_bom_component_fact",
    #         "sheet_name": "BOM_COMPONENT_FACT",
    #         "keys": [
    #               "PLANT_CODE",
    #               "COUNTRY_CODE",
    #               "PARENT_MATERIAL_ID",
    #               "COMPONENT_MATERIAL_ID",
    #               "VALID_FROM_DATE_KEY"
    #         ]
    #         },
    #   },

    # "Coty": 
    #     {
    #         "dim_geography_coty": {
    #             "table": "asper_production_coty_us_production_catalog.silver_us.star_dim_geography",
    #             "sheet_name": "Geo Dimension",
    #             "keys": ["GEOGRAPHY_DESCRIPTION"]
    #         },
    #         "dim_date_coty": {
    #             "table": "asper_production_coty_us_production_catalog.silver_us.star_dim_date",
    #             "sheet_name": "Date Dimension",
    #             "keys": ["CALENDAR_DATE"]
    #         },
    #         "dim_material_coty": {
    #             "table": "asper_production_coty_us_production_catalog.silver_us.star_dim_material",
    #             "sheet_name": "Material Dimension",
    #             "keys": ["UPC"]
    #         },
    #         "fact_pos_upc_coty": {
    #             "table": "asper_production_coty_us_production_catalog.silver_us.star_fact_pos_upc",
    #             "sheet_name": "Pos Fact (UPC)",
    #             "keys": ["GEOGRAPHY_DESCRIPTION", "WEEK_ENDING_DATE", "UPC"]
    #         }
    #     }

    # "CBI": {
    #         "material_dimension_circana": {
    #             "table": "asper_prod_cbi_databricks.silver.star_material_dimension",
    #             "sheet_name": "Material Dimension",
    #             "keys": ["UPC_13_DIGIT"]
    #         },
    #         "geo_dimension_circana": {
    #             "table": "asper_prod_cbi_databricks.silver.star_geo_dimension",
    #             "sheet_name": "Geo Dimension",
    #             "keys": ["GEOGRAPHY_KEY"]
    #         },
    #         "date_dimension_circana": {
    #             "table": "asper_prod_cbi_databricks.silver.star_date_dimension",
    #             "sheet_name": "Date Dimension",
    #             "keys": ["DATE_KEY"]
    #         },
    #         "pos_upc_fact_circana": {
    #             "table": "asper_prod_cbi_databricks.silver.star_pos_upc_fact",
    #             "sheet_name": "Pos Fact (UPC)",
    #             "keys": ["DATE_KEY", "UPC_13_DIGIT", "GEOGRAPHY_KEY"]
    #         }
    #     }

    #   "Perrigo_Romania": {
    #     "distribution_date_dim_romania": {
    #         "table": "asper_prod_uk_perrigo_romania_catlog.silver.star_distribution_date_dim",
    #         "sheet_name": "Date Dimension",
    #         "keys": ["DATE_KEY"]
    #     },
    #     "distribution_geo_dim_romania": {
    #         "table": "asper_prod_uk_perrigo_romania_catlog.silver.star_distribution_geo_dim",
    #         "sheet_name": "Geo Dimension",
    #         "keys": ["GEOGRAPHY_KEY"]
    #     },
    #     "distribution_material_dim_romania": {
    #         "table": "asper_prod_uk_perrigo_romania_catlog.silver.star_distribution_material_dim",
    #         "sheet_name": "Material Dimension",
    #         "keys": ["PRODUCT_KEY"]
    #     },
    #     "distribution_pos_dis_fact_romania": {
    #         "table": "asper_prod_uk_perrigo_romania_catlog.silver.star_distribution_pos_dis_fact",
    #         "sheet_name": "Pos Fact (Distribution)",
    #         "keys": ["GEOGRAPHY_DESCRIPTION", "UPC", "DATE_KEY"]
    #     }
    #     "sellout_date_dim_romania": {
    #         "table": "asper_prod_uk_perrigo_romania_catlog.silver.star_sellout_date_dim",
    #         "sheet_name": "Date Dimension",
    #         "keys": ["DATE_KEY"]
    #     },
    #     "sellout_material_dim_romania": {
    #         "table": "asper_prod_uk_perrigo_romania_catlog.silver.star_sellout_material_dim",
    #         "sheet_name": "Material Dimension",
    #         "keys": ["PRODUCT_KEY"]
    #     },
    #     "sellout_pos_fact_romania": {
    #         "table": "asper_prod_uk_perrigo_romania_catlog.silver.star_sellout_pos_fact",
    #         "sheet_name": "Pos Fact (UPC)",
    #         "keys": ["DATE_KEY", "NATIVE_PRODUCT_KEY", "UPC"]
    #     },
    #     "sellout_pos_fact_distribution_romania": {
    #         "table": "asper_prod_uk_perrigo_romania_catlog.silver.star_sellout_pos_fact_distribution",
    #         "sheet_name": "Pos Fact (Distribution)",
    #         "keys": ["DATE_KEY", "NATIVE_PRODUCT_KEY"]
    #     }
    #     "shipment_fact_romania": {
    #         "table": "asper_prod_uk_perrigo_romania_catlog.silver.star_shipment_fact",
    #         "sheet_name": "Shipment Fact",
    #         "keys": ["SHIPMENT_RECORD_ID"]
    #     },
    #     "organization_dim_romania": {
    #         "table": "asper_prod_uk_perrigo_romania_catlog.silver.star_organization_dim",
    #         "sheet_name": "Organization Dimension",
    #         "keys": ["ORGANIZATION_KEY"]
    #     }
    #   },

    #   "Perrigo_Itlay":
    #   {
    #       "sellout_ecomm_date_dim": {
    #   "table": "asper_prod_uk_perrigo_italy_catlog.silver.star_sellout_ecomm_date_dim",
    #   "sheet_name": "Date Dimension",
    #   "keys": ["DATE_ID"]
    # },
    # "sellout_ecomm_geo_dim": {
    #   "table": "asper_prod_uk_perrigo_italy_catlog.silver.star_sellout_ecomm_geo_dim",
    #   "sheet_name": "Geo Dimension",
    #   "keys": ["GEOGRAPHY_ID"]
    # },
    # "sellout_ecomm_channel_dim": {
    #   "table": "asper_prod_uk_perrigo_italy_catlog.silver.star_sellout_ecomm_channel_dim",
    #   "sheet_name": "Channel Dimension",
    #   "keys": ["CHANNEL_ID", "GEOGRAPHY_ID"]
    # },
    # "sellout_ecomm_product_dim": {
    #   "table": "asper_prod_uk_perrigo_italy_catlog.silver.star_sellout_ecomm_product_dim",
    #   "sheet_name": "Product Dimension",
    #   "keys": ["PRODUCT_ID"]
    # },
    # "sellout_ecomm_pos_fact": {
    #   "table": "asper_prod_uk_perrigo_italy_catlog.silver.star_sellout_ecomm_pos_fact",
    #   "sheet_name": "POS Fact (UPC)",
    #   "keys": [
    #     "DATE_ID",
    #     "GEOGRAPHY_ID",
    #     "CHANNEL_ID",
    #     "EAN_NUMBER",
    #     "UPC_12_DIGIT",
    #     "NATIVE_PRODUCT_ID"
    #   ]
    # },
    # "sellout_ecomm_pos_fact_overflow": {
    #   "table": "asper_prod_uk_perrigo_italy_catlog.silver.star_sellout_ecomm_pos_fact_overflow",
    #   "sheet_name": "POS Fact (UPC)",
    #   "keys": [
    #     "DATE_ID",
    #     "GEOGRAPHY_ID",
    #     "CHANNEL_ID",
    #     "EAN_NUMBER",
    #     "UPC_12_DIGIT",
    #     "NATIVE_PRODUCT_ID"
    #   ]
    # }
   
    #     "material_dim_sellout_italy": {
    #         "table": "asper_prod_uk_perrigo_italy_catlog.silver.star_material_dim_sellout",
    #         "sheet_name": "Material Dimension",
    #         "keys": [
    #             "MANUFACTURER_VALUE",
    #             "BRAND_NAME",
    #             "SUB_BRAND_NAME",
    #             "PRODUCT_DESCRIPTION",
    #             "PACK_SIZE",
    #             "CATEGORY_NAME",
    #             "SUBCATEGORY_VALUE"
    #         ]
    #     },
    #     "pos_distribution_fact_sellout_italy": {
    #         "table": "asper_prod_uk_perrigo_italy_catlog.silver.star_pos_distribution_fact_sellout",
    #         "sheet_name": "Pos Fact (Distribution)",
    #         "keys": ["NATIVE_PRODUCT_KEY", "DATE_KEY", "CHANNEL_CODE"]
    #     },
    #     "pos_upc_fact_sellout_italy": {
    #         "table": "asper_prod_uk_perrigo_italy_catlog.silver.star_pos_upc_fact_sellout",
    #         "sheet_name": "Pos Fact (UPC)",
    #         "keys": ["NATIVE_PRODUCT_KEY", "DATE_KEY", "CHANNEL_CODE"]
    #     },
    #     "date_dim_sellout_italy": {
    #         "table": "asper_prod_uk_perrigo_italy_catlog.silver.star_date_dim_sellout",
    #         "sheet_name": "Date Dimension",
    #         "keys": ["DATE_KEY"]
    #     },
    #     "channel_dim_sellout_italy": {
    #         "table": "asper_prod_uk_perrigo_italy_catlog.silver.star_channel_dim_sellout",
    #         "sheet_name": "Channel Dimension",
    #         "keys": ["CHANNEL_CODE"]
    #     },
    #     "inventory_fact_sellout_italy": {
    #         "table": "asper_prod_uk_perrigo_italy_catlog.silver.star_inventory_fact_sellout",
    #         "sheet_name": "Inventory Fact",
    #         "keys": ["INVENTORY_RECORD_KEY"]
    #     }
    #   },
         
    #         "organization_dim_italy": {
    #             "table": "asper_prod_uk_perrigo_italy_catlog.silver.star_organization_dim",
    #             "sheet_name": "Organization Dimension",
    #             "keys": ["ORGANIZATION_KEY"]
    #         },
    #         "shipment_fact_italy": {
    #             "table": "asper_prod_uk_perrigo_italy_catlog.silver.star_shipment_fact",
    #             "sheet_name": "Shipment Fact",
    #             "keys": ["SHIPMENT_RECORD_ID"]
    #         }
        
                
    #         "finance_fact_italy": {
    #             "table": "asper_prod_uk_perrigo_italy_catlog.silver.star_finance_fact",
    #             "sheet_name": "Finance Fact Posting",
    #             "keys": ["LEDGER_ENTRY_ID"]
    #         },
    #         "finance_fact_romania": {
    #             "table": "asper_prod_uk_perrigo_romania_catlog.silver.star_finance_fact",
    #             "sheet_name": "Finance Fact Posting",
    #             "keys": [
    #                 "COMPANY_CODE",
    #                 "DOCUMENT_NUMBER",
    #                 "DOCUMENT_DATE",
    #                 "GL_ACCOUNT",
    #                 "CUSTOMER_ID",
    #                 "NATIVE_PRODUCT_ID",
    #                 "BRAND_ID",
    #                 "SIGNED_AMOUNT_LOCAL"
    #             ]
            
    #     }
    #  }
 # "Perrigo_Germany": 
        #{
        # "sellout_ecomm_date_dim": {
        #   "table": "asper_prod_uk_perrigo_germany_catlog.silver.star_sellout_ecomm_date_dim",
        #   "sheet_name": "Date Dimension",
        #   "keys": ["DATE_ID"]
        # },
        # "sellout_ecomm_geo_dim": {
        #   "table": "asper_prod_uk_perrigo_germany_catlog.silver.star_sellout_ecomm_geo_dim",
        #   "sheet_name": "Geo Dimension",
        #   "keys": ["GEOGRAPHY_ID"]
        # },
        # "sellout_ecomm_channel_dim": {
        #   "table": "asper_prod_uk_perrigo_germany_catlog.silver.star_sellout_ecomm_channel_dim",
        #   "sheet_name": "Channel Dimension",
        #   "keys": ["CHANNEL_ID", "GEOGRAPHY_ID"]
        # },
        # "sellout_ecomm_product_dim": {
        #   "table": "asper_prod_uk_perrigo_germany_catlog.silver.star_sellout_ecomm_product_dim",
        #   "sheet_name": "Product Dimension",
        #   "keys": ["PRODUCT_ID"]
        # },
        # "sellout_ecomm_pos_fact": {
        #   "table": "asper_prod_uk_perrigo_germany_catlog.silver.star_sellout_ecomm_pos_fact",
        #   "sheet_name": "POS Fact (UPC)",
        #   "keys": ["DATE_ID", "NATIVE_PRODUCT_ID", "CATEGORY_NAME"]
        # },
        # "sellout_ecomm_pos_fact_overflow": {
        #   "table": "asper_prod_uk_perrigo_germany_catlog.silver.star_sellout_ecomm_pos_fact_overflow",
        #   "sheet_name": "POS Fact (UPC)",
        #   "keys": ["DATE_ID", "NATIVE_PRODUCT_ID", "CATEGORY_NAME"]
        # }
              
      #     "sellout_sales_product_dim": {
      #       "table": "asper_prod_uk_perrigo_germany_catlog.silver.star_sellout_sales_product_dim",
      #       "sheet_name": "Product Dimension",
      #       "keys": ["PRODUCT_ID"]
      #     },
      #     "sellout_sales_channel_dim": {
      #       "table": "asper_prod_uk_perrigo_germany_catlog.silver.star_sellout_sales_channel_dim",
      #       "sheet_name": "Channel Dimension",
      #       "keys": ["CHANNEL_DESCRIPTION"]
      #     },
      #     "sellout_sales_pos_fact": {
      #       "table": "asper_prod_uk_perrigo_germany_catlog.silver.star_sellout_sales_pos_fact",
      #       "sheet_name": "POS Fact (UPC)",
      #       "keys": ["NATIVE_PRODUCT_ID", "DATE_ID"]
      #     },
      #     "sellout_sales_pos_dis_fact": {
      #       "table": "asper_prod_uk_perrigo_germany_catlog.silver.star_sellout_sales_pos_dis_fact",
      #       "sheet_name": "POS Fact (Distribution)",
      #       "keys": ["NATIVE_PRODUCT_ID", "DATE_ID"]
      #     },
      #     "sellout_sales_inventory_fact": {
      #       "table": "asper_prod_uk_perrigo_germany_catlog.silver.star_sellout_sales_inventory_fact",
      #       "sheet_name": "Inventory Fact",
      #       "keys": ["NATIVE_PRODUCT_ID", "DATE_ID"]
      #     },
      #     "sellout_sales_overflow": {
      #       "table": "asper_prod_uk_perrigo_germany_catlog.silver.star_sellout_sales_overflow",
      #       "keys": ["NATIVE_PRODUCT_ID", "DATE_ID"]
      #     }
        
      
      # }
       
    # "Perrigo_Poland": 
    # {
    
    #         "sellout_mm_pos_fact_v2_poland": {
    #             "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_sellout_mm_pos_fact_v2",
    #             "sheet_name": "POS Fact (UPC)",
    #             "keys": [
    #                 "GEOGRAPHY_DESCRIPTION",
    #                 "NATIVE_PRODUCT_ID",
    #                 "WEEK_ENDING_DATE",
    #                 "CHANNEL_DESCRIPTION"
    #             ]
    #         },
    #         "sellout_mm_pos_dis_fact_v2_poland": {
    #             "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_sellout_mm_pos_dis_fact_v2",
    #             "sheet_name": "POS Fact (Distribution)",
    #             "keys": [
    #                 "GEOGRAPHY_DESCRIPTION",
    #                 "NATIVE_PRODUCT_ID",
    #                 "WEEK_ENDING_DATE",
    #                 "CHANNEL_ID"
    #             ]
    #         },
    #         "sellout_mm_product_dimension_v2_poland": {
    #             "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_sellout_mm_product_dimension_v2",
    #             "sheet_name": "Product Dimension",
    #             "keys": ["SKU_ID"]
    #         },
    #         "sellout_mm_channel_dim_v2_poland": {
    #             "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_sellout_mm_channel_dim_v2",
    #             "sheet_name": "Channel Dimension",
    #             "keys": ["GEOGRAPHY_DESCRIPTION", "CHANNEL_DESCRIPTION"]
    #         },
    #         "sellout_mm_geo_dim_v2_poland": {
    #             "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_sellout_mm_geo_dim_v2",
    #             "sheet_name": "Geo Dimension",
    #             "keys": ["GEOGRAPHY_DESCRIPTION"]
    #         },
    #         "sellout_mm_date_dim_v2_poland": {
    #             "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_sellout_mm_date_dim_v2",
    #             "sheet_name": "Date Dimension",
    #             "keys": ["DATE_ID"]
    #         }
    # }
    #             "transfer_discount_fact_sellon_poland": {
    #                 "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_transfer_discount_fact_sellon",
    #                 "sheet_name": "Shipment Fact",
    #                 "keys": ["DATE_KEY", "SHIP_TO_WAREHOUSE_ID", "CUSTOMER_ID", "NATIVE_PRODUCT_ID"]
    #             },
    #             "transfer_discount_distributor_dim_sellon_poland": {
    #                 "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_transfer_discount_distributor_dim_sellon",
    #                 "sheet_name": "Distributor Dimension",
    #                 "keys": ["DISTRIBUTOR_ID"]
    #             },
    #             "transfer_discount_date_dim_sellon_poland": {
    #                 "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_transfer_discount_date_dim_sellon",
    #                 "sheet_name": "Date Dimension",
    #                 "keys": ["DATE_KEY"]
    #             },
    #             "transfer_discount_retailer_dim_sellon_poland": {
    #                 "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_transfer_discount_retailer_dim_sellon",
    #                 "sheet_name": "Retailer Dimension",
    #                 "keys": ["CUSTOMER_KEY"]
    #             },
    #             "transfer_discount_material_dim_sellon_poland": {
    #                 "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_transfer_discount_material_dim_sellon",
    #                 "sheet_name": "Material Dimension",
    #                 "keys": ["PRODUCT_KEY"]
    #             }
    #         }
         
    #         "star_sellout_mm_pos_fact_poland": {
    #             "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_sellout_mm_pos_fact",
    #             "sheet_name": "Pos Fact (UPC)",
    #             "keys": ["GEOGRAPHY_KEY", "CHANNEL_DESCRIPTION", "UPC_13_DIGIT", "DATE_KEY"]
    #         },
    #         "star_sellout_mm_pos_dis_fact_poland": {
    #             "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_sellout_mm_pos_dis_fact",
    #             "sheet_name": "Pos Fact (Distribution)",
    #             "keys": ["GEOGRAPHY_DESCRIPTION", "CHANNEL_CODE", "UPC", "DATE_KEY"]
    #         },
    #         "dim_retailer_master_sellout_poland": {
    #             "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_dim_retailer_master_sellout",
    #             "sheet_name": "Retailer Dimension",
    #             "keys": ["CUSTOMER_KEY"]
    #         },
    #         "star_sellout_mm_material_dim_poland": {
    #             "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_sellout_mm_material_dim",
    #             "sheet_name": "Material Dimension",
    #             "keys": ["UPC_13_DIGIT"]
    #         },
    #         "dim_date_master_sellout_poland": {
    #             "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_dim_date_master_sellout",
    #             "sheet_name": "Date Dimension",
    #             "keys": ["DATE_KEY"]
    #         }
        
        
        #  "sellon_sap_mapping_standardized" : {
        #         "table": "asper_prod_uk_perrigo_poland_catlog.silver.sellon_sap_mapping_standardized",
        #         "keys": ["CODIGO_NACIONAL"],
        # },

        # "sellon_wholesale_on_invoice_mapping_standardized": {
        #     "table": "asper_prod_uk_perrigo_poland_catlog.silver.sellon_wholesale_on_invoice_mapping_standardized",
        #     "keys": ["DISTRIBUTOR_NAME"],
        # },

        # "sellon_pharmacy_mapping_standardized": {
        #     "table": "asper_prod_uk_perrigo_poland_catlog.silver.sellon_pharmacy_mapping_standardized",
        #     "keys": ["STORE_ID"],
        # },

        # "sellon_wholesale_sellon_mapping_standardized": {
        #     "table": "asper_prod_uk_perrigo_poland_catlog.silver.sellon_wholesale_sellon_mapping_standardized",
        #     "keys": ["TRANSFER_DISCOUNT_DISTRIBUTOR_ALIAS_NAME"],
        # },
         
            # "dim_date_master_sellout_poland": {
            #     "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_dim_date_master_sellout",
            #     "sheet_name": "Date Dimension",
            #     "keys": ["DATE_KEY"]
            # },
            # "dim_material_master_sellout_poland": {
            #     "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_dim_material_master_sellout",
            #     "sheet_name": "Material Dimension",
            #     "keys": ["MATERIAL_CODE"]
            # },
            # "dim_retailer_master_sellout_poland": {
            #     "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_dim_retailer_master_sellout",
            #     "sheet_name": "Retailer Dimension",
            #     "keys": ["CUSTOMER_KEY"]
            # },
            # "pos_fact_dis_sellout_distribution_poland": {
            #     "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_pos_fact_dis_SELLOUT_DISTRIBUTION",
            #     "sheet_name": "Pos Fact (Distribution)",
            #     # "keys": ["NATIVE_PRODUCT_KEY", "DATE_KEY", "CATEGORY_VALUE"]
            #     "keys": ["NATIVE_PRODUCT_KEY", "DATE_KEY"]
            # },
            # "sales_fact_sellout_poland": {
            #     "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_sales_fact_sellout",
            #     "sheet_name": "Pos Fact (UPC)",
            #     "keys": ["NATIVE_PRODUCT_KEY", "DATE_KEY", "GEOGRAPHY_DESCRIPTION"]
            # }
    #    },
          
           
    #         "bom_component_fact_poland": {
    #         "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_bom_component_fact",
    #         "sheet_name": "BOM_COMPONENT_FACT",
    #         "keys": [
    #               "PLANT_CODE",
    #               "COUNTRY_CODE",
    #               "PARENT_MATERIAL_ID",
    #               "COMPONENT_MATERIAL_ID",
    #                "VALID_FROM_DATE_KEY"
    #         ]
            
    #         },
    #     "product_dimension": {
    #             "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_product_dimension",
    #             "sheet_name": "Material Dimension",
    #             "keys": ["MATERIAL_CODE"],
    #               },
      #   "sellout_sales_fact": {
      #             "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_fact_sellout_sales",
      #             "sheet_name": "Pos Fact (UPC)",
      #             "keys": ["GEOGRAPHY_DESCRIPTION", "UPC", "WEEK_BEGINNING_DATE"]
      #             },
        # "customer_dimension": {
        #         "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_customer_dimension",
        #         "sheet_name": "Retailer Dimension",
        #         "keys": ["CUSTOMER_ID"],
        # },
#         "organization_dimension": {
#                 "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_organization_dimension",
#                 "sheet_name": "Organization Dimension",
#                 "keys": ["SALES_ORGANIZATION_CODE"],
#         },
        # "shipment_fact": {
        #         "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_shipment_fact",
        #         "sheet_name": "Shipment Fact",
        #         #"keys": ["INVOICE_ID", "NATIVE_PRODUCT_ID", "FISCAL_YEAR_PERIOD"],
        #         "keys": [
        #                     "SALES_ORGANIZATION_CODE",
        #                     "INVOICE_ID",
        #                     "INVOICE_DATE",
        #                     "NATIVE_PRODUCT_ID",
        #                     "SOLD_TO_CUSTOMER_ID",
        #                 ],
        #     },
        # "finance_fact": {
        #         "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_finance_fact",
        #         "sheet_name": "Finance Fact Posting",
        #         #"keys": ["COMPANY_CODE", "FISCAL_YEAR_PERIOD", "DOCUMENT_NUMBER"],
        #         "keys":[
        #                     "DOCUMENT_NUMBER",
        #                     "SIGNED_AMOUNT_LOCAL",
        #                     "NATIVE_PRODUCT_ID",
        #                     "BRAND_ID",
        #                     "CUSTOMER_ID",
        #                     "GL_ACCOUNT",
        #                     "DOCUMENT_DATE",
        #                     "FUNCTIONAL_AREA",
        #                 ],
        #             },
        # "list_price_fact": {
        #         "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_list_price_fact",
        #         "sheet_name": "List Price",
        #         "keys": [
        #         "SALES_ORGANIZATION_CODE",
        #         "CONDITION_TYPE_CODE",
        #         "CUSTOMER_ID",
        #         "MATERIAL_ID",
        #         "VALID_FROM_DATE",
        #         "PRICE_LIST_CODE",
        #         ],
        # },
          # "product_dimension": {
          #      "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_dim_material",
          #      "sheet_name": "Material Dimension",
          #      "keys": ["material_code", "week_beginning_date"]
          #      },
      #     "sellout_distribution_fact": {
      #          "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_fact_sellout_distribution",
      #          "sheet_name": "Pos Fact (UPC)",
      #          "keys": ["CATEGORY_NAME","UPC","WEEK_BEGINNING_DATE"]
      #          },
 
         # },


     
#
#     # ---------------------------
#     #  KIND
#     # ---------------------------
# "Kind": {
#     "star_geo_distribution_dimension": {
#       "table": "asper_production_kind_us_production_catalog.ppa_output.star_geo_distribution_dimension",
#       "sheet_name": "Geo Dimension",
#       "keys": ["GEOGRAPHY_KEY"]
#     },
#     "star_geo_sub_brand_dimension": {
#       "table": "asper_production_kind_us_production_catalog.ppa_output.star_geo_sub_brand_dimension",
#       "sheet_name": "Geo Dimension",
#       "keys": ["GEOGRAPHY_KEY"]
#     },
#     "star_geo_private_label_dimension": {
#       "table": "asper_production_kind_us_production_catalog.ppa_output.star_geo_private_label_dimension",
#       "sheet_name": "Geo Dimension",
#       "keys": ["GEOGRAPHY_KEY"]
#     },
#     "star_geo_iri_dimension": {
#       "table": "asper_production_kind_us_production_catalog.ppa_output.star_geo_iri_dimension",
#       "sheet_name": "Geo Dimension",
#       "keys": ["GEOGRAPHY_KEY"]
#     },
#     "star_time_distribution_dimension": {
#       "table": "asper_production_kind_us_production_catalog.ppa_output.star_time_distribution_dimension",
#       "sheet_name": "Date Dimension",
#       "keys": ["DATE_KEY"]
#     },
#     "star_time_iri_dimension": {
#       "table": "asper_production_kind_us_production_catalog.ppa_output.star_time_iri_dimension",
#       "sheet_name": "Date Dimension",
#       "keys": ["DATE_KEY"]
#     },
#     "star_time_private_level_dimension": {
#       "table": "asper_production_kind_us_production_catalog.ppa_output.star_time_private_level_dimension",
#       "sheet_name": "Date Dimension",
#       "keys": ["DATE_KEY"]
#     },
#     "star_time_sub_brand_dimension": {
#       "table": "asper_production_kind_us_production_catalog.ppa_output.star_time_sub_brand_dimension",
#       "sheet_name": "Date Dimension",
#       "keys": ["DATE_KEY"]
#     },
#     "star_product_distribution_dimension": {
#       "table": "asper_production_kind_us_production_catalog.ppa_output.star_product_distribution_dimension",
#       "sheet_name": "Material Dimension",
#       "keys": ["PRODUCT_KEY"]
#     },
#     "star_product_iri_dimension": {
#       "table": "asper_production_kind_us_production_catalog.ppa_output.star_product_iri_dimension",
#       "sheet_name": "Material Dimension",
#       "keys": ["PRODUCT_KEY"]
#     },
#     "star_product_private_label_dimension": {
#       "table": "asper_production_kind_us_production_catalog.ppa_output.star_product_private_label_dimension",
#       "sheet_name": "Material Dimension",
#       "keys": ["PRODUCT_KEY"]
#     },
#     "star_product_sub_brand_dimension": {
#       "table": "asper_production_kind_us_production_catalog.ppa_output.star_product_sub_brand_dimension",
#       "sheet_name": "Material Dimension",
#       "keys": ["PRODUCT_KEY"]
#     },
#     "star_pos_fact_upc": {
#       "table": "asper_production_kind_us_production_catalog.ppa_output.star_pos_fact_upc",
#       "sheet_name": "Pos Fact (UPC)",
#       "keys": ["UPC", "GEOGRAPHY_DESCRIPTION", "WEEK_ENDING_DATE"]
#     },
#     "star_pos_fact_distribution": {
#       "table": "asper_production_kind_us_production_catalog.ppa_output.star_pos_fact_distribution",
#       "sheet_name": "Pos Fact (Distribution)",
#       "keys": [
#         "CATEGORY_VALUE",
#         "SUBCATEGORY_VALUE",
#         "MANUFACTURER_VALUE",
#         "SUB_BRAND_VALUE",
#         "GEOGRAPHY_DESCRIPTION",
#         "WEEK_ENDING_DATE",
#         "COUNT_PER_PACK",
#         "NET_CONTENT_VALUE",
#         "BRAND_VALUE"
#       ]
#     },
#     "star_pos_fact_private_label": {
#       "table": "asper_production_kind_us_production_catalog.ppa_output.star_pos_fact_private_label",
#       "sheet_name": "Pos Fact (Subbrand)",
#       "keys": [
#         "CATEGORY_VALUE",
#         "SUBCATEGORY_VALUE",
#         "MANUFACTURER_VALUE",
#         "SUB_BRAND_VALUE",
#         "GEOGRAPHY_DESCRIPTION",
#         "WEEK_ENDING_DATE",
#         "COUNT_PER_PACK",
#         "NET_CONTENT_VALUE",
#         "BRAND_VALUE"
#       ]
#     },
#     "star_pos_fact_sub_brand": {
#       "table": "asper_production_kind_us_production_catalog.ppa_output.star_pos_fact_sub_brand",
#       "sheet_name": "Pos Fact (Subbrand)",
#       "keys": [
#         "CATEGORY_VALUE",
#         "SUBCATEGORY_VALUE",
#         "MANUFACTURER_VALUE",
#         "SUB_BRAND_VALUE",
#         "GEOGRAPHY_DESCRIPTION",
#         "WEEK_ENDING_DATE",
#         "BRAND_VALUE"
#       ]
#     },
#     "star_finance_fact": {
#       "table": "asper_production_kind_us_production_catalog.ppa_output.star_finance_fact",
#       "sheet_name": "Finance Fact KPI",
#       "keys": ["RETAILER_ID", "PPG_ID"]
#     },
#     "star_tpm_planning_fact": {
#       "table": "asper_production_kind_us_production_catalog.ppa_output.star_tpm_planning_fact",
#       "sheet_name": "TPM Planning Fact",
#       "keys": ["PROMOTION_ID", "SOURCE_PROMOTION_ID"]
#     }
#   },
#     "GeorgiaPacific":{
#         # "pos_fact_upc":{
#         #     "table": "asper_production_georgiapacific_us_catlog.ppa_output.star_pos_fact_upc",
#         #     "sheet_name": "Pos Fact (UPC)",
#         #     "keys": ["UPC", "GEOGRAPHY_DESCRIPTION", "WEEK_ENDING_DATE"]
#         # },
#         # "pos_fact_subbrand":{
#         #     "table": "asper_production_georgiapacific_us_catlog.ppa_output.star_pos_fact_subbrand",
#         #     "sheet_name": "Pos Fact (Subbrand)",
#         #
#         #     "keys": ["GEOGRAPHY_DESCRIPTION","WEEK_ENDING_DATE","CATEGORY_VALUE","MANUFACTURER_VALUE","BRAND_VALUE","SUB_BRAND_VALUE"]
#         # },
#         "tpm_planning_fact":
#         {
#             "table": "asper_production_georgiapacific_us_catlog.silver_us.star_tpm_planning_fact",
#             "sheet_name":"TPM Planning Fact",
#             "keys": ["MATERIAL_GROUP_ID", "PLAN_GROUP_ID", "PROMOTION_ID","START_TIMESTAMP","END_TIMESTAMP"]
#         },
#         # "product_dimension":
#         # {
#         #     "table": "asper_production_georgiapacific_us_catlog.ppa_output.star_product_dimension",
#         #     "sheet_name": "Material Dimension",
#         #     "keys": ["UPC"]
#         #
#         # },
#         "pos_fact_upc": {
#             "table": "asper_production_georgiapacific_us_catlog.ppa_data.star_pos_fact_upc",
#             "sheet_name": "Pos Fact (UPC)",
#             "keys": ["UPC", "GEOGRAPHY_DESCRIPTION", "WEEK_ENDING_DATE"]
#         },
#         "pos_fact_subbrand": {
#             "table": "asper_production_georgiapacific_us_catlog.ppa_data.star_pos_fact_subbrand",
#             "sheet_name": "Pos Fact (Subbrand)",
#
#             "keys": ["GEOGRAPHY_DESCRIPTION", "WEEK_ENDING_DATE", "CATEGORY_VALUE", "MANUFACTURER_VALUE", "BRAND_VALUE",
#                      "SUB_BRAND_VALUE"]
#         },
#     },
#
#
#      "Hormel":{

#             "material_dimension": {
#                 "table": "asper_production_hormelfoods_us_production_catalog.silver_us.star_dim_material",
#                 "sheet_name": "Material Dimension",
#                 "keys": ["UPC"]
#             },
# #
#             "geo_dimension": {
#                 "table": "asper_production_hormelfoods_us_production_catalog.silver_us.star_dim_geo",
#                 "sheet_name": "Geo Dimension",
#                 "keys": ["GEOGRAPHY_KEY"]
#             },
#
#             "date_day_dimension": {
#                 "table": "asper_production_hormelfoods_us_production_catalog.silver_us.star_dim_date_day",
#                 "sheet_name": "Date Dimension",
#                 "keys": ["CURRENT_YEAR_CALENDAR_DATE"]
#             },
#
            #  "date_week_dimension": {
            #     "table": "asper_production_hormelfoods_us_production_catalog.silver_us.star_dim_date_fiscal_week",
            #      "sheet_name": "Date Dimension",
            #      "keys": ["FISCAL_YEAR_WEEK"]
            #  },
#
            # "retailer_dimension": {
            #     "table": "asper_production_hormelfoods_us_production_catalog.silver_us.star_dim_retailer",
            #     "sheet_name": "Retailer Dimension",
            #     "keys": ["PLANT_TO_ID"]
            # },

#
#
#             "customer_planto_dimension": {
#                 "table": "asper_production_hormelfoods_us_production_catalog.silver_us.star_dim_customer_planto",
#                 "sheet_name": "Planned Customer Dimension",
#                 "keys": ["PLANNED_CUSTOMER_CODE"]
#             },
#
#             "customer_dimension": {
#                 "table": "asper_production_hormelfoods_us_production_catalog.silver_us.star_dim_customer",
#                 "sheet_name": "Retailer Dimension",
#                 "keys": ["DISTRIBUTOR_CUSTOMER_CODE"]
#             },
#
#             "tpm_promotion_dimension": {
#                 "table": "asper_production_hormelfoods_us_production_catalog.silver_us.star_dim_tpm_promotion",
#                 "sheet_name": "TPM Promotion Dimension",
#                 "keys": ["PROMOTION_ID", "PPG_ID"]
#             },
#
#             "tpm_plan_dimension": {
#                 "table": "asper_production_hormelfoods_us_production_catalog.silver_us.star_dim_tpm_plan",
#                 "sheet_name": "TPM Plan Dimension",
#                 "keys": ["PLAN_ID"]
#             },
#
        # "shipment_fact": {
        #     "table": "asper_production_hormelfoods_us_production_catalog.silver_us.star_fact_shipment",
        #     "sheet_name": "Shipment Fact",
        #      "keys": ["SKU_ID", "SHIP_TO_WAREHOUSE_ID", "DATE_KEY", "UPC"]
        #  },
        # "fact_pos_upc": {
        #     "table": "asper_production_hormelfoods_us_production_catalog.silver_us.star_fact_pos_upc",
        #     "sheet_name": "Pos Fact (UPC)",
        #     #"keys": ["SKU_ID", "SHIP_TO_WAREHOUSE_ID", "DATE_KEY", "UPC"]
        #     "keys": ["UPC", "GEOGRAPHY_DESCRIPTION", "WEEK_ENDING_DATE"]
        #      },
        
#         "star_fact_tpm_planning":{
#             "table": "asper_production_hormelfoods_us_production_catalog.silver_us.star_fact_tpm_planning",
#             "sheet_name": "TPM Planning Fact",
#             "keys": ["CUSTOMER_KEY", "PPG_ID", "PROMOTION_ID", "WEEK_KEY", "PLAN_ID"]
#         },
#         "star_fact_tpm_accrual":{
#             "table": "asper_production_hormelfoods_us_production_catalog.silver_us.star_fact_tpm_accrual",
#             "sheet_name": "TPM Accrual Fact",
#             "keys": ["CUSTOMER_KEY", "PPG_ID", "PROMOTION_ID", "WEEK_KEY", "PLAN_ID"]
#
#         },
#
#         # ---------------------------
#         # DIMENSIONS
#         # ---------------------------
#         "date_dimension": {
#             "table": "asper_production_hormelfoods_us_production_catalog.silver_us.star_dim_date_fiscal_week",
#             "sheet_name": "Date Dimension",
#             "keys": ["FISCAL_YEAR_WEEK"]
#         },
#
     #     "product_dimension": {
     #        "table": "asper_production_hormelfoods_us_production_catalog.silver_us.star_dim_product",
     #        "sheet_name": "Material Dimension",
     #        "keys": ["PPG", "PRODUCT_KEY"]
     #     },
          
        #  "plant_geo_bridge": {
        #      "table": "asper_production_hormelfoods_us_production_catalog.silver_us.star_bridge_planto_geo",
        #      "sheet_name": "PLANT_TO_GEOGRAPHY_BRIDGE",
        #      "keys": ["PLANT_TO_ID", "GEOGRAPHY_DESCRIPTION", "CUSTOMER_DIVISION_KEY"]
        #  },

        #  },
     #    "product_finance_dimension": {
     #    "table": "asper_production_hormelfoods_us_production_catalog.silver_us.star_dim_product_finance",
     #    "sheet_name": "Material Dimension",
     #    "keys": ["PRODUCT_KEY"]
     #    },
        
    #     "finance_fact": {
    #     "table": "asper_production_hormelfoods_us_production_catalog.silver_us.star_fact_finance",
    #     "sheet_name": "Finance Fact KPI",
    #     "keys": ["PLANT_TO_ID", "FISCAL_WEEK", "PRODUCT_KEY","FISCAL_YEAR"]
    #     },
        
    #     "planto_geo_finance_bridge": {
    #     "table": "asper_production_hormelfoods_us_production_catalog.silver_us.star_bridge_planto_geo_finance",
    #     "sheet_name": "PLANT_TO_GEOGRAPHY_BRIDGE",
    #     "keys": ["PLANT_TO_ID", "CUSTOMER_DIVISION_KEY"]
    #     },
    #   },
    # "Perrigo_Poland": {
    #     "sellout_mm_date_dim_poland": {
    #         "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_sellout_mm_date_dim",
    #         "sheet_name": "Date Dimension",
    #         "keys": ["DATE_KEY"]
    #     },
    #     "sellout_mm_material_dim_poland": {
    #         "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_sellout_mm_material_dim",
    #         "sheet_name": "Material Dimension",
    #         "keys": ["UPC_13_DIGIT"]
    #     },
    #     "sellout_mm_pos_dis_fact_poland": {
    #         "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_sellout_mm_pos_dis_fact",
    #         "sheet_name": "Pos Fact (Distribution)",
    #         "keys": ["GEOGRAPHY_DESCRIPTION", "CHANNEL_CODE", "UPC", "DATE_KEY"]
    #     },
    #     "sellout_mm_pos_fact_poland": {
    #         "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_sellout_mm_pos_fact",
    #         "sheet_name": "Pos Fact (UPC)",
    #         "keys": ["GEOGRAPHY_KEY", "CHANNEL_DESCRIPTION", "UPC_13_DIGIT", "DATE_KEY"]
    #     },
    #     "sellout_mm_retailer_dim_poland": {
    #         "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_sellout_mm_retailer_dim",
    #         "sheet_name": "Retailer Dimension",
    #         "keys": ["CUSTOMER_KEY"]
    #     }
    #         "sales_customer_sellout_stock_poland": {
    #         "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_sales_customer_sellout_stock",
    #         "sheet_name": "Retailer Dimension",
    #         "keys": ["CUSTOMER_KEY"]
    #         },
    #         "sales_date_dim_sellout_stock_poland": {
    #             "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_sales_date_dim_sellout_stock",
    #             "sheet_name": "Date Dimension",
    #             "keys": ["DATE_KEY"]
    #         },
    #         "sales_fact_sellout_poland": {
    #             "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_sales_fact_sellout",
    #             "sheet_name": "Pos Fact (UPC)",
    #             "keys": ["NATIVE_PRODUCT_KEY", "DATE_KEY", "GEOGRAPHY_DESCRIPTION"]
    #         },
    #         "sales_material_dim_sellout_stock_poland": {
    #             "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_sales_material_dim_sellout_stock",
    #             "sheet_name": "Material Dimension",
    #             "keys": ["MATERIAL_CODE"]
    #         },
    #         "stock_inventory_fact_poland": {
    #             "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_stock_inventory_fact",
    #             "sheet_name": "Inventory Fact",
    #             "keys": ["MATERIAL_ID", "CHANNEL_DESCRIPTION", "WEEK_BEGINNING_DATE"]
    #         },
    #         "dim_date_stock_poland": {
    #             "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_dim_date_stock",
    #             "sheet_name": "Date Dimension",
    #             "keys": ["DATE_KEY"]
    #         },
    #         "dim_material_stock_poland": {
    #             "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_dim_material_stock",
    #             "sheet_name": "Material Dimension",
    #             "keys": ["MATERIAL_CODE"]
    #         },
    #         "sales_retailer_dim_sellout_stock_poland": {
    #             "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_sales_retailer_dim_sellout_stock",
    #             "sheet_name": "Retailer Dimension",
    #             "keys": ["CUSTOMER_KEY"]
    #         }
     #      "fact_sellon_poland": {
     #           "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_fact_sellon",
     #           "sheet_name": "Shipment Fact",
     #           "keys": ["SHIPMENT_RECORD_ID"]
     #           },
     #      "store_dimension_poland": {
     #           "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_store_dimension",
     #           "sheet_name": "Store Dimension",
     #           "keys": ["STORE_ID"]
     #           },
     #      "dim_material_sellon_poland": {
     #           "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_dim_material_sellon",
     #           "sheet_name": "Material Dimension",
     #           "keys": ["MATERIAL_CODE"]
     #           },
     #      "channel_dimension_poland": {
     #           "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_channel_dimension",
     #           "sheet_name": "Channel Dimension",
     #           "keys": ["CHANNEL_CODE"]
     #           },
     #      "date_dimension_poland": {
     #           "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_date_dimension",
     #           "sheet_name": "Date Dimension",
     #           "keys": ["CALENDAR_DATE"]
     #           },
     #      "distributor_dimension_poland": {
     #           "table": "asper_prod_uk_perrigo_poland_catlog.silver.star_distributor_dimension",
     #           "sheet_name": "Distributor Dimension",
     #           "keys": ["DISTRIBUTOR_NAME"]
     #           },
#           "product_dimension": {
#           "table": "asper_prod_uk_perrigo_france_catlog.silver_us.star_product_dimension",
#           "sheet_name": "Material Dimension",
#           "keys": ["MATERIAL_CODE"]
#           },
#           "customer_dimension": {
#           "table": "asper_prod_uk_perrigo_france_catlog.silver_us.star_customer_dimension",
#           "sheet_name": "Retailer Dimension",
#           "keys": ["CUSTOMER_ID"]
#           },
#           "organization_dimension": {
#           "table": "asper_prod_uk_perrigo_france_catlog.silver_us.star_organization_dimension",
#           "sheet_name": "Organization Dimension",
#           "keys": ["SALES_ORGANIZATION_CODE"]
#           },
#           "shipment_fact": {
#           "table": "asper_prod_uk_perrigo_france_catlog.silver_us.star_shipment_fact",
#           "sheet_name": "Shipment Fact",
#           "keys": ["INVOICE_ID", "NATIVE_PRODUCT_ID", "INVOICE_DATE"]
#           },
#           "finance_fact_posting": {
#           "table": "asper_prod_uk_perrigo_france_catlog.silver_us.star_finance_fact",
#           "sheet_name": "Finance Fact Posting",
#           "keys": ["COMPANY_CODE", "FISCAL_YEAR_PERIOD", "DOCUMENT_NUMBER"]
#           },
    #   },
     #  "JDE_PNP_US_Production": {
        # "dim_week_jde": {
        #     "table": "asper_production_jde_pnp_us_production_catalog.silver.star_dim_week",
        #     "sheet_name": "Date Dimension",
        #     "keys": ["CALENDAR_YEAR", "CALENDAR_WEEK"]
        # },
        # "dim_material_jde": {
        #     "table": "asper_production_jde_pnp_us_production_catalog.silver.star_dim_material",
        #     "sheet_name": "Material Dimension",
        #     "keys": ["UPC"]
        # },
        # "dim_channel_jde": {
        #     "table": "asper_production_jde_pnp_us_production_catalog.silver.star_dim_channel",
        #     "sheet_name": "Channel Dimension",
        #     "keys": ["CHANNEL_CODE"]
        # },
        # "dim_geo_jde": {
        #     "table": "asper_production_jde_pnp_us_production_catalog.silver.star_dim_geo",
        #     "sheet_name": "Geo Dimension",
        #     "keys": ["REGION_NAME"]
        # },
        # "fact_nielsen_jde": {
        #     "table": "asper_production_jde_pnp_us_production_catalog.silver.star_fact_nielsen",
        #     "sheet_name": "Pos Fact (UPC)",
        #     "keys": ["SOURCE", "DATE_KEY", "CHANNEL_DESCRIPTION", "REGION_NAME", "UPC"]
        # }
        # "fact_nielsen_distribution_jde": {
        #     "table": "asper_production_jde_pnp_us_production_catalog.silver.star_fact_nielsen_distribution",
        #     "sheet_name": "POS Fact (Distribution)",
        #     "keys": ["EAN_NO", "DATE_ID", "CHANNEL_ID", "GEOGRAPHY_ID"]
        # }
    #         "product_dimension_nielsen": {
    #             "table": "asper_production_jde_pnp_us_production_catalog.silver.star_product_dimension_nielsen",
    #             "sheet_name": "Material Dimension",
    #             "keys": ["EAN_NO"]
    #             },
     #      "neogrid_carrefour_fact": {
     #      "table": "asper_production_jde_pnp_us_production_catalog.silver.star_fact_neogrid_carrefour",
     #      "sheet_name": "Pos Fact (UPC)",
     #      "keys": ["SOURCE", "DATE_KEY", "STORE_ID", "UPC"]
     #      },
     #      "nielsen_fact": {
     #      "table": "asper_production_jde_pnp_us_production_catalog.silver.star_fact_nielsen",
     #      "sheet_name": "Pos Fact (UPC)",
     #      "keys": ["SOURCE", "DATE_KEY", "CHANNEL_DESCRIPTION", "REGION_NAME", "UPC"]
     #      },
     #      "date_dimension": {
     #      "table": "asper_production_jde_pnp_us_production_catalog.silver.star_dim_date",
     #      "sheet_name": "Date Dimension",
     #      "keys": ["CALENDAR_DATE"]
     #      },
     #      "week_dimension": {
     #      "table": "asper_production_jde_pnp_us_production_catalog.silver.star_dim_week",
     #      "sheet_name": "Date Dimension",
     #      "keys": ["CALENDAR_YEAR", "CALENDAR_WEEK"]
     #      },
     #      "product_dimension": {
     #      "table": "asper_production_jde_pnp_us_production_catalog.silver.star_dim_material",
     #      "sheet_name": "Material Dimension",
     #      "keys": ["UPC"]
     #      },
     #      "channel_dimension": {
     #      "table": "asper_production_jde_pnp_us_production_catalog.silver.star_dim_channel",
     #      "sheet_name": "Channel Dimension",
     #      "keys": ["CHANNEL_CODE"]
     #      },
     #      "geo_dimension": {
     #      "table": "asper_production_jde_pnp_us_production_catalog.silver.star_dim_geo",
     #      "sheet_name": "Geo Dimension",
     #      "keys": ["REGION_NAME"]
     #      },
     #      "store_dimension": {
     #      "table": "asper_production_jde_pnp_us_production_catalog.silver.star_dim_store",
     #      "sheet_name": "Store Dimension",
     #      "keys": ["STORE_ID"]
     #      },
     #      "customer_dimension": {
     #      "table": "asper_production_jde_pnp_us_production_catalog.silver.star_dim_retailer",
     #      "sheet_name": "Retailer Dimension",
     #      "keys": ["BANNER_NAME"]
     #      },
     #       },
            
   
}



#  Common schema file
#SCHEMA_PATH = "/Volumes/asper_prod_uk_perrigo_poland_catlog/test/silver_schema_file/ADP Silver Schema-29-04-26.xlsx"
#SCHEMA_PATH = "/Volumes/asper_prod_uk_perrigo_france_catlog/silver_us/adp_silver_sheet" 

#SCHEMA_PATH = "/Volumes/asper_prod_uk_perrigo_poland_catlog/silver/adp_silver_sheet/ADP_Silver_Schema_V2 (4).xlsx"

#SCHEMA_PATH = "/Volumes/asper_prod_cbi_databricks/silver/adp_silver_sheet/ADP Silver Schema (9).xlsx"

#SCHEMA_PATH = "/Volumes/asper_production_coty_us_production_catalog/silver_us/adp_silver_sheet/ADP Silver Schema.xlsx"

#SCHEMA_PATH = "/Volumes/asper_production_danone_us_production_catalog/silver_us/silver_volume/ADP Silver Schema (16).xlsx"

#SCHEMA_PATH = "/Volumes/asper_production_jde_pnp_us_production_catalog/silver/adp_silver_sheet/ADP Silver Schema (20).xlsx"

#SCHEMA_PATH = "/Volumes/asper_production_jde_pnp_us_production_catalog/silver/adp_silver_sheet/ADP_Silver_Schema_V2.xlsx"



#SCHEMA_PATH = "/Volumes/asper_prod_uk_perrigo_france_catlog/silver/adp_silver_sheet"

SCHEMA_PATH = "/Volumes/asper_production_asper_us_dp_dev_catalog/test_atul/volume/ADP_Silver.xlsx"

#SCHEMA_PATH = "/Volumes/asper_production_hormelfoods_us_production_catalog/silver_us/rgm_output/ADP Silver Schema (15).xlsx"

#SCHEMA_PATH = "/Volumes/asper_product_databricks_catlog/test/silver_schema/ADP Silver Schema-29-04-26.xlsx"
POS_Derivation_Analysis ="/Volumes/asper_product_databricks_catlog/test/silver_schema/POS Schema Derivation Analysis.xlsx"


#/Volumes/asper_product_databricks_catlog/test/silver_schema/ADP Silver Schema (1).xlsx

#"/Volumes/asper_product_databricks_catlog/test/silver_schema/ADP Silver Schema-29-04-26.xlsx"