import importlib
import pkgutil
import inspect


def list_cls_under_mod(mod, cls, uq_attr):
    """
    List classes including derived under one module (imported).
    Parameters:
        mod: imported module
        cls: Base class
        uq_attr: name of the class member that each class own a unique value
    Return:
        A dict that represents a class.
    """
    r = dict()
    for loader, name, ispkg in pkgutil.walk_packages(
            getattr(mod, '__path__', None), prefix=mod.__name__ + '.'):
        if ispkg is False:
            mod = importlib.import_module(name)
            for name, c in inspect.getmembers(mod, inspect.isclass):
                if issubclass(c, cls):
                    v = getattr(c, uq_attr)
                    if v is not None:
                        r[v] = c
    return r


def get_track_keywords(site_tuples):
    k = []
    # for site_id, site_domain in site_tuples:
    #     if site_domain.startswith('www.'):
    #         site_domain = site_domain[4:]
    #         lst_dict = {"value": site_domain.replace('.', ' ')}
    #     # k.append(site_domain.replace('.', ' '))
    #     k.append(lst_dict)
    for i in range(1, len(site_tuples)):
        if i <= 25:  # This condition will be removed, it's there for testing purposes.
            site: str = site_tuples[i]
            site_dct = {"value": 'url:' + site}
        k.append(site_dct)
    return k
