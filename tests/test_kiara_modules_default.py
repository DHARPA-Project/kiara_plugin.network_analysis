#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `kiara_plugin.network_analysis` package."""

import pytest  # noqa

import kiara_plugin.network_analysis


def test_assert():

    assert kiara_plugin.network_analysis.get_version() is not None
