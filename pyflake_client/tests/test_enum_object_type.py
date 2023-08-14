# -*- coding: utf-8 -*-

from pyflake_client.models.enums.object_type import ObjectType


def test_object_type_table_serialization():
    """test_object_type_table_serialization"""
    assert str(ObjectType.TABLE) == "TABLE"
    assert ObjectType.TABLE.singularize() == "TABLE"
    assert ObjectType.TABLE.pluralize() == "TABLES"


def test_object_type_view_serialization():
    """test_object_type_view_serialization"""
    assert str(ObjectType.VIEW) == "VIEW"
    assert ObjectType.VIEW.singularize() == "VIEW"
    assert ObjectType.VIEW.pluralize() == "VIEWS"


def test_object_type_matview_serialization():
    """test_object_type_matview_serialization"""
    assert str(ObjectType.MATVIEW) == "MATERIALIZED VIEW"
    assert ObjectType.MATVIEW.singularize() == "MATERIALIZED VIEW"
    assert ObjectType.MATVIEW.pluralize() == "MATERIALIZED VIEWS"
