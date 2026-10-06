# import pytest
# from pyspark.sql import SparkSession
# from pyspark.dbutils import DBUtils
#
# # @pytest.fixture(scope="session")
# # def spark():
# #     """
# #     Reuse the active Databricks Spark session
# #     """
#
# #     spark = SparkSession.getActiveSession()
#
# #     if spark is None:
# #         spark = SparkSession.builder.getOrCreate()
#
# #     # provide spark to tests
# #     yield spark
#
#
# import pytest
# from pyspark.sql import SparkSession
#
# SOURCE_STORAGE_ACCOUNT = "asperkindsnacks"
# storage_key = ""
# RGM_PATH = "wasbs://pricing-promo@asperkindsnacks.blob.core.windows.net/version11/semantic_layer/20260211/upc_level_data.csv"
#
#
# @pytest.fixture(scope="session")
# def spark():
#     spark = SparkSession.getActiveSession()
#     return spark
#
#
# @pytest.fixture(scope="session")
# def azure_storage(spark):
#     """
#     Configure Azure storage credentials once
#     """
#     # dbutils = DBUtils(spark)
#
#     spark.conf.set(
#         f"fs.azure.account.key.{SOURCE_STORAGE_ACCOUNT}.blob.core.windows.net",
#         storage_key
#     )
#
#     return True
#
#
# @pytest.fixture(scope="session")
# def rgm_df(spark, azure_storage):
#     """
#     Load RGM dataset once
#     """
#
#     return spark.read.csv(
#         RGM_PATH,
#         header=True,
#         inferSchema=True
#     )
#

import pytest
from utils.data_loader import load_input_data
from test_data.dataset_config import DATASETS, SCHEMA_PATH
from etl_test.utils.config_helper import get_all_datasets
from etl_test.test_data.dataset_config import DATASETS

def pytest_addoption(parser):
    parser.addoption(
        "--brand",
        action="store",
        default="ALL",
        help="Brand to run (Danone, Kind, ALL)"
    )
    parser.addoption("--dataset", action="store", default="ALL")


# def pytest_generate_tests(metafunc):
#     if "input_data" in metafunc.fixturenames:
#         brand = metafunc.config.getoption("brand")
#
#         datasets = get_all_datasets(DATASETS, brand)
#
#         metafunc.parametrize("input_data", datasets, indirect=True)
#
#


def pytest_generate_tests(metafunc):
    if "input_data" in metafunc.fixturenames:

        brand = metafunc.config.getoption("brand")
        dataset = metafunc.config.getoption("dataset")

        datasets = get_all_datasets(DATASETS, brand, dataset)

        if not datasets:
            raise ValueError(f"No datasets found for brand={brand}, dataset={dataset}")

        metafunc.parametrize(
            "input_data",
            datasets,
            indirect=True,
            ids=[f"{c}-{d}" for c, d, _ in datasets]
        )


@pytest.fixture(scope="session")
def brand(request):
    return request.config.getoption("--brand")


@pytest.fixture(scope="module")
def input_data(request):

    customer, dataset, details = request.param

    df, base_df = load_input_data(
        details["table"],
        details.get("sheet_name"),
        SCHEMA_PATH,
    )

    # 🔹 Return keys also
    return df, base_df, customer, dataset, details