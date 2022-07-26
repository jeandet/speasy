from ._xml_catalogs_parser import load_xml_catalog
from ._cdf_masters_parser import update_tree


def build_inventory(xml_file_path: str, master_cdf_path: str):
    t = load_xml_catalog(xml_file_path)
    update_tree(master_cdf_dir=master_cdf_path)
    return t
