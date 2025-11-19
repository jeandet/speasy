"""
.. testsetup:: *

   from speasy.core.algorithms import *
   import numpy as np
"""

import random
from typing import Any, Dict, Callable, List, Tuple
from functools import wraps

"""
Global shared thread pool executor
"""


def _thread_pool_executor():
    from concurrent.futures import ThreadPoolExecutor
    from speasy.config import core as core_config
    if not hasattr(_thread_pool_executor, "executor"):
        _thread_pool_executor.executor = ThreadPoolExecutor(core_config.max_concurrent_requests.get())
    return _thread_pool_executor.executor


def ishuffle(items: List[Any]) -> List[Tuple[int, Any]]:
    """Shuffles the given list and returns a list of tuples (original_index, item)
    Parameters
    ----------
    items: list
        list of items to shuffle
    Returns
    -------
    list
        A list of tuples (original_index, item) in randomized order
    """
    indexed_list = list(enumerate(items))
    random.shuffle(indexed_list)
    return indexed_list


def imap(f: Callable, items: List[Tuple[int, Any]], *args, **kwargs) -> List[Tuple[int, Any]]:
    """Applies function f to all elements in the given list of tuples (original_index, item)
    Parameters
    ----------
    f: Callable
        function to apply to each element
    items: list
        list of tuples (original_index, item)
    args: Any
        additional positional arguments to pass to f
    kwargs: Any
        additional keyword arguments to pass to f
    Returns
    -------
    list
        A list of tuples (original_index, f(item)) in the same order as input
    """
    return [(i, f(e, *args, **kwargs)) for i, e in items]


def ipmap(f: Callable, items: List[Tuple[int, Any]], *args, **kwargs) -> List[Tuple[int, Any]]:
    """Applies function f to all elements in the given list of tuples (original_index, item) in parallel
    Parameters
    ----------
    f: Callable
        function to apply to each element
    items: list
        list of tuples (original_index, item)
    args: Any
        additional positional arguments to pass to f
    kwargs: Any
        additional keyword arguments to pass to f
    Returns
    -------
    list
        A list of tuples (original_index, f(item)) in the same order as input
    """
    from concurrent.futures import as_completed
    result = []
    executor = _thread_pool_executor()
    future_to_index = {executor.submit(f, e, *args, **kwargs): i for i, e in items}
    for future in as_completed(future_to_index):
        i = future_to_index[future]
        result.append((i, future.result()))
    return result


def isort(items: List[Tuple[int, Any]]) -> List[Any]:
    """Sorts the given list of tuples (original_index, item) by original_index
    Parameters
    ----------
    items: list
        list of tuples (original_index, item)
    Returns
    -------
    list
        A list of items sorted by original_index
    """
    return [e for i, e in sorted(items, key=lambda x: x[0])]


"""
The rationale behind the following function is to randomize the order of execution so we minimize the requests collisions and maximize the throughput.
"""


def randomized_map(f: Callable, l, *args, **kwargs):
    """Applies function f to all elements in list l in a randomized order

    Parameters
    ----------
    f: Callable
        function to apply to each element in l
    l: list
        list of elements to process
    args: Any
        additional positional arguments to pass to f
    kwargs: Any
        additional keyword arguments to pass to f

    Returns
    -------
    list
        A list with the results of applying f to each element in l, in the original order

    Examples
    --------
    >>> randomized_map(lambda x: x**2, [1,2,3,4])
    [1, 4, 9, 16]
    """
    if not len(l):
        return []
    return isort(imap(f, ishuffle(l), *args, **kwargs))


def randomized_pmap(f: Callable, l, *args, **kwargs):
    """Applies function f to all elements in list l in a randomized order in parallel

    Parameters
    ----------
    f: Callable
        function to apply to each element in l
    l: list
        list of elements to process
    args: Any
        additional positional arguments to pass to f
    kwargs: Any
        additional keyword arguments to pass to f

    Returns
    -------
    list
        A list with the results of applying f to each element in l, in the original order

    Examples
    --------
    >>> randomized_pmap(lambda x: x**2, [1,2,3,4])
    [1, 4, 9, 16]
    """
    if not len(l):
        return []
    return isort(ipmap(f, ishuffle(l), *args, **kwargs))


def pack_kwargs(**kwargs: Any) -> Dict:
    """Packs given keyword arguments into a dictionary

    Parameters
    ----------
    kwargs: Any
        Any keyword argument is accepted

    Returns
    -------
    dict
        A dict with all kwargs packed

    Examples
    --------
    >>> pack_kwargs(a=1, b="2")
    {'a': 1, 'b': '2'}
    """
    return kwargs


class AllowedKwargs(object):
    """A decorator that prevent from passing unexpected kwargs to a function
    """

    def __init__(self, allowed_list):
        self.allowed_list = set(allowed_list)

    def __call__(self, func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            unexpected_args = list(
                filter(lambda arg_name: arg_name not in self.allowed_list, kwargs.keys()))
            if not unexpected_args:
                return func(*args, **kwargs)
            raise TypeError(
                f"Unexpected keyword argument {unexpected_args}, allowed keyword arguments are {self.allowed_list}")

        return wrapped


def fix_name(name: str):
    """Makes given input compatible with python charset https://docs.python.org/3/reference/lexical_analysis.html#identifiers

    Parameters
    ----------
    name: str
        input string to sanitize

    Returns
    -------
    str
        a string compatible with python naming rules


    Examples
    --------
    >>> fix_name('Parker Solar Probe (PSP)')
    'Parker_Solar_Probe_PSP'

    >>> fix_name('IS⊙ISEPI_Lo')
    'ISoISEPI_Lo'

    >>> fix_name('all_Legal_strings_123')
    'all_Legal_strings_123'

    """
    rules = (
        ('-', '_'),
        (':', '_'),
        ('.', '_'),
        ('(', ''),
        (')', ''),
        ('/', ''),
        (' ', '_'),
        ('{', ''),
        ('}', ''),
        ('(', ''),
        ('⊙', 'o'),
        (';', '_'),
        (',', '_'),
        ('%', '_')
    )
    if len(name):
        if name[0].isnumeric():
            name = "n_" + name
        for bad, replacement in rules:
            if bad in name:
                name = name.replace(bad, replacement)
        return name
    raise ValueError("Got empty name")
