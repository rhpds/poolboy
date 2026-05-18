#!/usr/bin/env python

import logging
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock

sys.path.append('../operator')

import kubernetes_asyncio.client.exceptions


class FakeApiException(kubernetes_asyncio.client.exceptions.ApiException):
    def __init__(self, status):
        self.status = status


K8sApiException = kubernetes_asyncio.client.exceptions.ApiException


class FakeResourceClaim:
    """Minimal ResourceClaim stub matching the new wrapper pattern in resourceclaim.py."""

    def __init__(self, update_side_effects):
        self._update_impl = AsyncMock(side_effect=update_side_effects)
        self.refetch = AsyncMock()
        self.refetch_count = 0

        original_refetch = self.refetch
        async def counting_refetch():
            self.refetch_count += 1
            await original_refetch()
        self.refetch = counting_refetch

    async def update_status_from_handle(self, logger, resource_handle, resource_states):
        """Public wrapper with retry — mirrors resourceclaim.py."""
        attempt = 0
        while True:
            try:
                await self._update_impl(
                    logger=logger,
                    resource_handle=resource_handle,
                    resource_states=resource_states,
                )
                break
            except K8sApiException as exception:
                if exception.status == 422 and attempt <= 10:
                    await self.refetch()
                    attempt += 1
                else:
                    raise

    def __str__(self):
        return "ResourceClaim test-claim"


class TestResourceClaimStatusRetry(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.logger = logging.getLogger('test')
        self.resource_handle = MagicMock()
        self.resource_states = [{'kind': 'AnarchySubject', 'metadata': {'name': 'test-as'}}]

    async def test_success_no_retry(self):
        """Successful update should not trigger any retry."""
        rc = FakeResourceClaim(update_side_effects=[None])
        await rc.update_status_from_handle(
            self.logger, self.resource_handle, self.resource_states,
        )
        rc._update_impl.assert_called_once()
        self.assertEqual(rc.refetch_count, 0)

    async def test_422_retries_and_succeeds(self):
        """422 on first call should refetch and retry, then succeed."""
        rc = FakeResourceClaim(update_side_effects=[
            FakeApiException(status=422),
            FakeApiException(status=422),
            None,
        ])
        await rc.update_status_from_handle(
            self.logger, self.resource_handle, self.resource_states,
        )
        self.assertEqual(rc._update_impl.call_count, 3)
        self.assertEqual(rc.refetch_count, 2)

    async def test_422_exhausts_retries(self):
        """More than 10 consecutive 422s should raise the exception."""
        rc = FakeResourceClaim(
            update_side_effects=[FakeApiException(status=422)] * 12
        )
        with self.assertRaises(K8sApiException) as ctx:
            await rc.update_status_from_handle(
                self.logger, self.resource_handle, self.resource_states,
            )
        self.assertEqual(ctx.exception.status, 422)
        self.assertEqual(rc._update_impl.call_count, 12)
        self.assertEqual(rc.refetch_count, 11)

    async def test_404_raises_without_retry(self):
        """404 is not retried — it propagates immediately (in production, caught inside __update_status_from_handle)."""
        rc = FakeResourceClaim(update_side_effects=[FakeApiException(status=404)])
        with self.assertRaises(K8sApiException) as ctx:
            await rc.update_status_from_handle(
                self.logger, self.resource_handle, self.resource_states,
            )
        self.assertEqual(ctx.exception.status, 404)
        rc._update_impl.assert_called_once()
        self.assertEqual(rc.refetch_count, 0)

    async def test_other_exception_raises_immediately(self):
        """Non-422/404 exceptions should raise immediately."""
        rc = FakeResourceClaim(update_side_effects=[FakeApiException(status=500)])
        with self.assertRaises(K8sApiException) as ctx:
            await rc.update_status_from_handle(
                self.logger, self.resource_handle, self.resource_states,
            )
        self.assertEqual(ctx.exception.status, 500)
        rc._update_impl.assert_called_once()
        self.assertEqual(rc.refetch_count, 0)


if __name__ == '__main__':
    unittest.main()
