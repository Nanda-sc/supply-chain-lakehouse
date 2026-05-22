import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RAW_DATA_PATH = os.path.join(BASE_DIR, "data", "raw", "DataCoSupplyChainDataset.csv")

BRONZE_PATH = os.path.join(BASE_DIR, "bronze", "supply_chain")
SILVER_PATH = os.path.join(BASE_DIR, "silver", "supply_chain")
GOLD_PATH                    = os.path.join(BASE_DIR, "gold")
GOLD_DELIVERY_PERFORMANCE    = os.path.join(BASE_DIR, "gold", "delivery_performance")
GOLD_SUPPLIER_PERFORMANCE    = os.path.join(BASE_DIR, "gold", "supplier_performance")
GOLD_SHIPPING_ANALYSIS       = os.path.join(BASE_DIR, "gold", "shipping_analysis")

SOURCE_FILE_NAME = "DataCoSupplyChainDataset.csv"
