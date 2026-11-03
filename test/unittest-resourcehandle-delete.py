#!/usr/bin/env python3
"""Offline regression tests for finalizer-aware, reverse-order teardown."""
import logging
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'operator'))

import kopf
from kubernetes_asyncio.client import ApiException
from resourcehandle import ResourceHandle


class TestResourceHandleDelete(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.resources = {
            name: {'metadata': {'name': name, 'annotations': {}}}
            for name in ('cnv', 'aws', 'workload')
        }
        self.handle = ResourceHandle(
            annotations={}, labels={}, meta={}, name='guid-test', namespace='poolboy',
            spec={}, uid='test-uid', status={'resources': [
                {'reference': {'apiVersion': 'anarchy.gpte.redhat.com/v1',
                               'kind': 'AnarchySubject', 'namespace': 'anarchy', 'name': name}}
                for name in self.resources
            ]},
        )
        self.deleted = []
        self.immediate = False

        async def get_object(**kwargs):
            if kwargs['name'] not in self.resources:
                raise ApiException(status=404)
            return self.resources[kwargs['name']]

        async def delete_object(**kwargs):
            name = kwargs['name']
            self.deleted.append(name)
            if self.immediate:
                self.resources.pop(name)
            else:
                self.resources[name]['metadata']['deletionTimestamp'] = '2026-11-03T22:17:00Z'

        self.get = self.enterContext(patch('poolboy_k8s.get_object', side_effect=get_object))
        self.annotate = self.enterContext(patch('poolboy_k8s.patch_object', new_callable=AsyncMock))
        self.delete = self.enterContext(patch('poolboy_k8s.delete_object', side_effect=delete_object))
        self.claim = self.enterContext(patch.object(self.handle, 'get_resource_claim',
                                                  new=AsyncMock(return_value=None)))

    async def reconcile(self):
        await self.handle.handle_delete(logger=logging.getLogger(__name__))

    async def test_workload_finalizer_blocks_backing_clusters_across_reconciles(self):
        with self.assertRaises(kopf.TemporaryError):
            await self.reconcile()
        self.assertEqual(self.deleted, ['workload'])
        self.claim.assert_not_awaited()
        with self.assertRaises(kopf.TemporaryError):
            await self.reconcile()
        self.assertEqual(self.deleted, ['workload'])  # Do not resubmit DELETE.
        self.assertEqual(self.annotate.await_count, 1)

        del self.resources['workload']  # Anarchy finished its destroy job.
        with self.assertRaises(kopf.TemporaryError):
            await self.reconcile()
        self.assertEqual(self.deleted, ['workload', 'aws'])
        del self.resources['aws']
        with self.assertRaises(kopf.TemporaryError):
            await self.reconcile()
        self.assertEqual(self.deleted, ['workload', 'aws', 'cnv'])
        del self.resources['cnv']
        await self.reconcile()
        self.claim.assert_awaited_once()

    async def test_immediate_deletion_finishes_in_one_reconcile(self):
        self.immediate = True
        await self.reconcile()
        self.assertEqual(self.deleted, ['workload', 'aws', 'cnv'])
        self.claim.assert_awaited_once()

    async def test_missing_and_uncreated_resources_are_skipped(self):
        del self.resources['workload']
        self.handle.status['resources'].append({})
        self.immediate = True
        await self.reconcile()
        self.assertEqual(self.deleted, ['aws', 'cnv'])

    async def test_read_error_does_not_delete_backing_resources(self):
        self.get.side_effect = ApiException(status=403)
        with self.assertRaises(ApiException):
            await self.reconcile()
        self.delete.assert_not_awaited()
        self.claim.assert_not_awaited()

    async def test_read_error_after_delete_does_not_advance(self):
        self.get.side_effect = [self.resources['workload'], ApiException(status=503)]
        with self.assertRaises(ApiException):
            await self.reconcile()
        self.assertEqual(self.deleted, ['workload'])
        self.claim.assert_not_awaited()

    async def test_annotation_error_does_not_start_any_delete(self):
        self.annotate.side_effect = ApiException(status=403)
        with self.assertRaises(ApiException):
            await self.reconcile()
        self.delete.assert_not_awaited()
        self.claim.assert_not_awaited()

    async def test_empty_handle_finishes(self):
        self.handle.status = {}
        await self.reconcile()
        self.get.assert_not_awaited()
        self.delete.assert_not_awaited()
        self.claim.assert_awaited_once()

    async def test_delete_error_does_not_delete_backing_resources(self):
        self.delete.side_effect = ApiException(status=500)
        with self.assertRaises(ApiException):
            await self.reconcile()
        self.assertEqual(self.delete.await_args.kwargs['name'], 'workload')
        self.assertEqual(self.delete.await_count, 1)
        self.claim.assert_not_awaited()

    async def test_disappearance_between_read_and_delete_is_safe(self):
        self.delete.side_effect = ApiException(status=404)
        await self.reconcile()
        self.assertEqual([c.kwargs['name'] for c in self.delete.await_args_list],
                         ['workload', 'aws', 'cnv'])


if __name__ == '__main__':
    unittest.main()
