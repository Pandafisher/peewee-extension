# coding: utf-8

import zlib
import simplejson
from peewee import BlobField, NotSupportedError, DataError


class DataTooBigError(DataError):
    pass


class InvalidValueError(DataError):
    pass


class InvalidHeaderError(DataError):
    pass


class CompBlobField(BlobField):

    BLOB_MAX_LENGTH = 1024 * 64 - 1
    COMP_THRESHOLD = 1024 * 2
    COMP_HEADER = 'z'
    DECOMP_HEADER = 'x'

    def __init__(self, *args, **kwargs):
        null = kwargs.pop('null', False)
        if null is True:
            raise NotSupportedError("%s type collumn must NOT NULL!" % self.__class__.__name__)
        default = kwargs.pop('default', '')
        CompBlobField.check(default)
        super(CompBlobField, self).__init__(null=null, default=default, *args, **kwargs)

    @classmethod
    def check(cls, value):
        if value is None or not isinstance(value, (basestring, bytearray)):
            raise InvalidValueError

    def db_value(self, value):
        CompBlobField.check(value)
        if len(value) > self.COMP_THRESHOLD:
            s = self.COMP_HEADER + zlib.compress(value)
        else:
            s = self.DECOMP_HEADER + value
        if len(s) > self.BLOB_MAX_LENGTH:
            raise DataTooBigError("\nOriginal length: %d\nAfter Compression length: %d\nData: %s\n" % (len(value), len(s), str(value)))
        return super(CompBlobField, self).db_value(s)

    def python_value(self, value):
        value = super(CompBlobField, self).python_value(value)
        header, data = value[0], value[1:]
        if header == self.COMP_HEADER:
            return zlib.decompress(data)
        elif header == self.DECOMP_HEADER:
            return data
        else:
            raise InvalidHeaderError


class JSONField(CompBlobField):

    def db_value(self, value):
        value = simplejson.dumps(value)
        return super(JSONField, self).db_value(value)

    def python_value(self, value):
        value = super(JSONField, self).python_value(value)
        return simplejson.loads(value)
