#!/usr/bin/env python

import asyncio
import logging
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.append('../operator')

import kubernetes_asyncio.client.exceptions


class FakeApiException(kubernetes_asyncio.client.exceptions.ApiException):
    def __init__(self, status):
        self.status = status


class FakeResourceClaim:
    """Minimal ResourceClaim stub for testing retry behavior."""

    def __init__(self, update_side_effects):
        self.update_status_from_handle = AsyncMock(side_effect=update_side_effects)
        self.refetch = AsyncMock()
        self.refetch_count = 0

        original_refetch = self.refetch
        async def counting_refetch():
            self.refetch_count += 1
            await original_refetch()
        self.refetch = counting_refetch

    def __str__(self):
        return "ResourceClaim test-claim"


K8sApiException = kubernetes_asyncio.client.exceptions.ApiException


async def retry_update_status_from_handle(logger, resource_claim, resource_handle, resource_states):
    """Extracted retry logic matching the pattern in resourcehandle.py."""
    attempt = 0
    while True:
        try:
            await resource_claim.update_status_from_handle(
                logger=logger,
                resource_handle=resource_handle,
                resource_states=resource_states,
            )
            break
        except K8sApiException as exception:
            if exception.status == 404:
                logger.info("Ignoring update on deleted %s", resource_claim)
                break
            elif exception.status == 422 and attempt <= 10:
                await resource_claim.refetch()
                attempt += 1
            else:
                raise


class TestResourceClaimStatusRetry(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.logger = logging.getLogger('test')
        self.resource_handle = MagicMock()
        self.resource_states = [{'kind': 'AnarchySubject', 'metadata': {'name': 'test-as'}}]

    async def test_success_no_retry(self):
        """Successful update should not trigger any retry."""
        rc = FakeResourceClaim(update_side_effects=[None])
        await retry_update_status_from_handle(
            self.logger, rc, self.resource_handle, self.resource_states,
        )
        rc.update_status_from_handle.assert_called_once()
        self.assertEqual(rc.refetch_count, 0)

    async def test_422_retries_and_succeeds(self):
        """422 on first call should refetch and retry, then succeed."""
        rc = FakeResourceClaim(update_side_effects=[
            FakeApiException(status=422),
            FakeApiException(status=422),
            None,
        ])
        await retry_update_status_from_handle(
            self.logger, rc, self.resource_handle, self.resource_states,
        )
        self.assertEqual(rc.update_status_from_handle.call_count, 3)
        self.assertEqual(rc.refetch_count, 2)

    async def test_422_exhausts_retries(self):
        """More than 10 consecutive 422s should raise the exception."""
        rc = FakeResourceClaim(
            update_side_effects=[FakeApiException(status=422)] * 12
        )
        with self.assertRaises(K8sApiException) as ctx:
            await retry_update_status_from_handle(
                self.logger, rc, self.resource_handle, self.resource_states,
            )
        self.assertEqual(ctx.exception.status, 422)
        self.assertEqual(rc.update_status_from_handle.call_count, 12)
        self.assertEqual(rc.refetch_count, 11)

    async def test_404_breaks_without_retry(self):
        """404 should break immediately without retry."""
        rc = FakeResourceClaim(update_side_effects=[FakeApiException(status=404)])
        await retry_update_status_from_handle(
            self.logger, rc, self.resource_handle, self.resource_states,
        )
        rc.update_status_from_handle.assert_called_once()
        self.assertEqual(rc.refetch_count, 0)

    async def test_other_exception_raises_immediately(self):
        """Non-422/404 exceptions should raise immediately."""
        rc = FakeResourceClaim(update_side_effects=[FakeApiException(status=500)])
        with self.assertRaises(K8sApiException) as ctx:
            await retry_update_status_from_handle(
                self.logger, rc, self.resource_handle, self.resource_states,
            )
        self.assertEqual(ctx.exception.status, 500)
        rc.update_status_from_handle.assert_called_once()
        self.assertEqual(rc.refetch_count, 0)


if __name__ == '__main__':
    unittest.main()
