"""Publish, deliver and retry paths found only in rejected historical candidates."""
from itertools import permutations, product
import unittest
from unittest.mock import patch

from hgraph_fabric.data_structures import DataUpdateDetails
from test_dependency_revision_resolution import World


def waiting_reader(order=('a', 'c', 'b'), counts=False, target=7):
    world = World()
    source = world.process('source')
    source.publish('x', 3)
    gate = world.process('gate', ['x'])
    gate.publish('gate', 100)
    middle = world.process('middle', ['x', 'gate'])
    middle.publish('m', 100)
    world.process('original n', ['m']).publish('n', 100)
    for key in ('a', 'c'):
        source.publish(key, 100)

    source.publish('x', 7)
    gate.publish('gate', 200)
    if target != 7:
        source.publish('x', target)
    peer = world.process('peer', ['x'])
    peer.publish('b', 400)
    source.publish('x', 9)
    # N restarts with a different input chain, retaining its value. M's real
    # input cut stays at X@7 because gate@200 still consumes X@7.
    world.process('restarted n', ['x']).publish('n')
    for key in ('a', 'c'):
        world.process(key, ['n']).publish(key, None if counts else 200)
    reader = world.process('reader', order)
    return world, source, middle, peer, reader


class HistoricalConflictWatchTests(unittest.TestCase):
    def test_historical_path_notification_advances_waiting_roots(self):
        for order, counts, omission in product(permutations(('a', 'c', 'b')), (False, True), (False, True)):
            with self.subTest(order=order, counts=counts, omission=omission):
                world, _, middle, _, reader = waiting_reader(order, counts)
                self.assertEqual(reader.cut(), {'a': ('a', 100, 0), 'c': ('c', 100, 0), 'b': ('b', 400, 0)})
                self.assertEqual(middle.cut()['x'], ('x', 7, 0))
                # The shared-cache image changes before every manager receives
                # its notification; no private retry or root republication.
                publisher = world.process('m without inputs') if omission else middle
                publisher.publish('m')
                expected = (100, 1) if counts else (200, 0)
                self.assertEqual(reader.cut(), {'a': ('a', *expected), 'c': ('c', *expected), 'b': ('b', 400, 0)})
                for key in ('a', 'c'):
                    choices = reader.subscriptions[key].current_dependency_revisions
                    self.assertEqual(choices[('n', 100)], ('n', 100, 0))
                    self.assertEqual(choices[('m', 100)], ('m', 100, 1))
                self.assertEqual(reader.session._subscriptions._conflicts, {})
                with patch.object(reader.session._subscriptions, '_try_advance') as retry:
                    publisher.publish('m')  # Duplicate notification from the harness.
                retry.assert_not_called()

    def test_path_remains_watched_when_both_current_and_latest_omit_it(self):
        world, _, middle, _, reader = waiting_reader()
        # Latest roots now refer directly to X@9. A/C@200 remain admissible
        # forward candidates, but N and M appear in neither endpoint tree.
        for key in ('a', 'c'):
            world.process(f'direct {key}', ['x']).publish(key, 300)
        middle.publish('m')
        self.assertEqual(reader.cut(), {'a': ('a', 200, 0), 'c': ('c', 200, 0), 'b': ('b', 400, 0)})
        self.assertEqual(reader.nodes()['x'], 7)

    def test_historical_watch_retires_after_candidate_is_below_current(self):
        world, source, middle, peer, reader = waiting_reader()
        producers = [world.process(f'direct {key}', ['x']) for key in ('a', 'c')]
        for key, producer in zip(('a', 'c'), producers):
            producer.publish(key, 300)
        middle.publish('m')
        peer.publish('b', 500)
        self.assertEqual(reader.cut(), {'a': ('a', 300, 0), 'c': ('c', 300, 0), 'b': ('b', 500, 0)})
        source.publish('x', 11)
        for key, producer in zip(('a', 'c'), producers):
            producer.publish(key, 400)
        before = reader.cut()
        with patch.object(reader.session._subscriptions, '_try_advance') as retry:
            world.process('retired m').publish('m')
        retry.assert_not_called()
        self.assertEqual(reader.cut(), before)

    def test_delayed_final_revision_keeps_watch_after_successor_notification(self):
        _, _, middle, _, reader = waiting_reader()
        queued = []
        with patch.object(reader.session, 'apply_update', side_effect=queued.append):
            middle.publish('m')
            middle.publish('m', 200)
        self.assertEqual([update.revision for update in queued], [(100, 1), (200, 0)])
        self.assertEqual(reader.session.apply_update(queued[1]), {})
        # The new version's header says M@100 finished at r1, but this cache
        # has only observed r0. The older notification can still unblock it.
        changes = reader.session.apply_update(queued[0])
        self.assertEqual(set(changes), {'a', 'c'})
        self.assertEqual(reader.cut(), {'a': ('a', 200, 0), 'c': ('c', 200, 0), 'b': ('b', 400, 0)})

    def test_fully_observed_sealed_version_does_not_keep_a_historical_watch(self):
        _, _, middle, _, reader = waiting_reader()
        middle.publish('m', 200)  # M@100 remains at r0 permanently.
        manager = reader.session._subscriptions
        self.assertNotIn(('m', 100), manager._conflicts)
        self.assertTrue(all(('m', 100) not in versions for versions in manager._historical_conflicts.values()))

    def test_blocked_retry_ignores_duplicates_but_wakes_on_the_next_revision(self):
        world, _, middle, _, reader = waiting_reader(target=8)
        before = reader.cut()
        with patch.object(reader.session, 'apply_update', wraps=reader.session.apply_update) as receive:
            middle.publish('m')  # X@7 still cannot match B's X@8.
        update = receive.call_args.args[0]
        self.assertEqual(reader.cut(), before)
        details = reader.cache.get_revision(('m', 100, 0))
        older = DataUpdateDetails(id='m', revision=(100, 0), dependencies=details.dependencies,
                                  weak_dependencies=details.weak_dependencies,
                                  previous_revision=details.previous_revision, as_of=details.as_of)
        with patch.object(reader.session._subscriptions, '_try_advance') as retry:
            self.assertEqual(reader.session.apply_update(update), {})
            self.assertEqual(reader.session.apply_update(older), {})
        retry.assert_not_called()
        # B still consumes X@8; the gate producer's actual cut therefore
        # selects X@8 too, even though the source has already published X@9.
        world.process('later gate', ['x', 'b']).publish('gate', 300)
        middle.publish('m')
        self.assertEqual(reader.cut(), {'a': ('a', 200, 0), 'c': ('c', 200, 0), 'b': ('b', 400, 0)})
        self.assertEqual(reader.nodes()['x'], 8)

    def test_dependency_removal_uses_watch_count_before_retiring_final_revision(self):
        world, _, _, _, reader = waiting_reader()
        publisher = world.process('m without dependencies')
        queued = []
        with patch.object(reader.session, 'apply_update', side_effect=queued.append):
            publisher.publish('m')  # M@100/r1 removes X and G.
            publisher.publish('m', 200)
        reader.session.apply_update(queued[1])
        manager = reader.session._subscriptions
        owners = {root[0] for root, versions in manager._historical_conflicts.items() if ('m', 100) in versions}
        self.assertEqual(owners, {'a', 'c'})
        retry = manager._try_advance
        observed = []

        def check_before_retirement(sub):
            # At this point the cache holds the final count. Cleaning watches
            # now would remove both historical owners and wrongly close the gate.
            self.assertFalse(manager._revision_may_advance(('m', 100), ('m', 100, 1)))
            observed.append(sub.data_id)
            return retry(sub)

        with patch.object(manager, '_try_advance', side_effect=check_before_retirement):
            self.assertEqual(set(reader.session.apply_update(queued[0])), {'a', 'c'})
        self.assertTrue(observed)
        self.assertEqual(reader.cut(), {'a': ('a', 200, 0), 'c': ('c', 200, 0), 'b': ('b', 400, 0)})
        self.assertEqual(manager._historical_conflicts, {})
        self.assertEqual(manager._conflicts, {})

    def test_removing_conflict_endpoint_keeps_the_consumers_notification_eligible(self):
        world, _, _, _, reader = waiting_reader()
        manager = reader.session._subscriptions
        self.assertEqual(manager._dependents[('x', 9)], {'a', 'c'})
        retry = manager._try_advance
        observed = []

        def check_after_index_refresh(sub):
            # N's update removes both paths to X@9. The gate uses N's consumer
            # watch, not the remaining number of paths to the conflict endpoint.
            self.assertNotIn(('x', 9), manager._dependents)
            self.assertEqual(manager._dependents[('n', 100)], {'a', 'c'})
            observed.append(sub.data_id)
            return retry(sub)

        with patch.object(manager, '_try_advance', side_effect=check_after_index_refresh):
            world.process('n without dependencies').publish('n')
        self.assertTrue(observed)
        self.assertEqual(reader.cut(), {'a': ('a', 200, 0), 'c': ('c', 200, 0), 'b': ('b', 400, 0)})


if __name__ == '__main__':
    unittest.main()
