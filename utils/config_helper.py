# def get_all_datasets(config):
#     """
#     Convert DATASETS config into list of tuples
#     (customer, dataset, details)
#     """
#
#     result = []
#
#     for customer, datasets in config.items():
#         for dataset, details in datasets.items():
#             result.append((customer, dataset, details))
#
#     return result

# def get_all_datasets(DATASETS, brand_filter="ALL"):
#     dataset_list = []
#
#     # Convert input → list
#     if isinstance(brand_filter, str):
#         brand_filter = [b.strip().lower() for b in brand_filter.split(",")]
#
#     for customer, datasets in DATASETS.items():
#
#         # ✅ Apply filter
#         if "all" not in brand_filter and customer.lower() not in brand_filter:
#             continue
#
#         for dataset, details in datasets.items():
#             dataset_list.append((customer, dataset, details))
#
#     return dataset_list


# def get_all_datasets(DATASETS, brand_filter="ALL"):
#     dataset_list = []
#
#     # Normalize
#     if not brand_filter:
#         brand_filter = ["all"]
#     else:
#         brand_filter = [b.strip().lower() for b in brand_filter.split(",")]
#
#     for customer, datasets in DATASETS.items():
#
#         # ✅ Skip only if NOT ALL and NOT matching
#         if "all" not in brand_filter and customer.lower() not in brand_filter:
#             continue
#
#         for dataset, details in datasets.items():
#             dataset_list.append((customer, dataset, details))
#
#     return dataset_list

def has_schema_sheet(details):
    """Return True when a non-empty sheet_name is configured for ADP schema lookup."""
    return bool(details.get("sheet_name"))


def get_all_datasets(DATASETS, brand_filter="ALL", dataset_filter="ALL"):
    dataset_list = []

    # Normalize brand
    if not brand_filter:
        brand_filter = ["all"]
    else:
        brand_filter = [b.strip().lower() for b in brand_filter.split(",")]

    # Normalize dataset
    if not dataset_filter:
        dataset_filter = ["all"]
    else:
        dataset_filter = [d.strip().lower() for d in dataset_filter.split(",")]

    for customer, datasets in DATASETS.items():

        # Brand filter
        if "all" not in brand_filter and customer.lower() not in brand_filter:
            continue


        for dataset, details in datasets.items():

            # Dataset filter
            if "all" not in dataset_filter and dataset.lower() not in dataset_filter:
                continue

            dataset_list.append((customer, dataset, details))

    return dataset_list