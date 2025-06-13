#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `kiara_plugin.develop` package."""

import pytest  # noqa

import kiara_plugin.develop


def test_assert():
    assert kiara_plugin.develop.get_version() is not None
