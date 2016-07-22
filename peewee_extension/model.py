# coding: utf-8
from peewee import Model as _Model, DoesNotExist


class Model(_Model):

    @classmethod
    def get(cls, *query, **kwargs):
        try:
            return super(Model, cls).get(*query, **kwargs)
        except DoesNotExist:
            return None
