#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `kiara_modules_default` package."""

import pytest  # noqa

import kiara_modules.network_analysis


def test_assert():

    assert kiara_modules.network_analysis.get_version() is not None
