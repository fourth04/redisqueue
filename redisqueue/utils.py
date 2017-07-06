import six
import hashlib
from importlib import import_module
from pkgutil import iter_modules


def bytes_to_str(s, encoding='utf-8'):
    """Returns a str if a bytes object is given."""
    if six.PY3 and isinstance(s, bytes):
        return s.decode(encoding)
    return s


def load_object(path):
    """Load an object given its absolute object path, and return it.

    object can be a class, function, variable or an instance.
    path ie: 'scrapy.downloadermiddlewares.redirect.RedirectMiddleware'
    """

    try:
        dot = path.rindex('.')
    except ValueError:
        raise ValueError("Error loading object '%s': not a full path" % path)

    module, name = path[:dot], path[dot+1:]
    mod = import_module(module)

    try:
        obj = getattr(mod, name)
    except AttributeError:
        raise NameError(
            "Module '%s' doesn't define any object named '%s'" %
            (module, name))

    return obj


def walk_modules(path):
    """Loads a module and all its submodules from the given module path and
    returns them. If *any* module throws an exception while importing, that
    exception is thrown back.

    For example: walk_modules('scrapy.utils')
    """

    mods = []
    mod = import_module(path)
    mods.append(mod)
    if hasattr(mod, '__path__'):
        for _, subpath, ispkg in iter_modules(mod.__path__):
            fullpath = path + '.' + subpath
            if ispkg:
                mods += walk_modules(fullpath)
            else:
                submod = import_module(fullpath)
                mods.append(submod)
    return mods

def to_bytes(text, encoding=None, errors='strict'):
    """Return the binary representation of `text`. If `text`
    is already a bytes object, return it as-is."""
    if isinstance(text, bytes):
        return text
    if not isinstance(text, six.string_types):
        raise TypeError('to_bytes must receive a unicode, str or bytes '
                        'object, got %s' % type(text).__name__)
    if encoding is None:
        encoding = 'utf-8'
    return text.encode(encoding, errors)

def obj_fingerprint(obj):
    """将一个对象hash化

    :obj: TODO
    :returns: TODO

    """
    fp = hashlib.sha1(to_bytes(str(obj))).digest()
    return fp
