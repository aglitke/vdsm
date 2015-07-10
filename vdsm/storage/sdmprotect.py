#
# Copyright 2015 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
#
# Refer to the README and COPYING files for full details of the license
#

from functools import wraps
from threadLocal import vars


class SDMFunctionNotCallable(Exception):
    pass


def require_sdm(f):
    """
    SDM method decorator.

    This decorator is used to mark methods that can only be called when the
    storage is operating in SDM mode (ie. without SPM).
    """
    @wraps(f)
    def wrapper(*args, **kwds):
        if getattr(vars, '__sdm__', False):
            return f(*args, **kwds)
        else:
            raise SDMFunctionNotCallable(f.__name__)
    return wrapper


def sdm_verb(f):
    """
    SDM verb decorator

    This decorator indicates that a function is designed to work without SPM
    and is approved to access SDM-only functions.
    """
    @wraps(f)
    def wrapper(*args, **kwds):
        vars.__sdm__ = True
        return f(*args, **kwds)
    return wrapper
