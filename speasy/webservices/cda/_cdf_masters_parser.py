import os.path
import logging
from ...inventory import flat_inventories

log = logging.getLogger(__name__)


def extract_variable(variable):
    return {
        'shape': variable.shape[1:],
        'attributes': {name: str(value) for name, value in variable.attrs.items()}
    }


def extract_variables(cdf):
    return {
        name: extract_variable(v) for name, v in cdf.items()
    }


def load_master_cdf(path, dataset):
    try:
        from spacepy.pycdf import CDF
        cdf = CDF(path)
        variables=extract_variables(cdf)

    except ImportError:
        log.warning(
            "Can't import spacepy.pycdf you are not supposed to use this function unless you are running a speasy proxy")


def update_tree(master_cdf_dir):
    for dataset in flat_inventories.cda.datasets.values():
        master_cdf_fname = dataset.mastercdf.split('/')[-1]
        full_path = os.path.join(master_cdf_dir, master_cdf_fname)
        if os.path.exists(full_path):
            load_master_cdf(full_path, dataset)
