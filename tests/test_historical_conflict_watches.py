"""Publish, deliver and retry paths found only in rejected historical candidates."""
from itertools import permutations, product
import unittest
from unittest.mock import patch

from hgraph_fabric.data_structures import DataUpdateDetails
from test_dependency_revision_resolution import World


def waiting_reader(order=('a', 'c', 'b'), counts=False, target=2):
    world = World()
    source = world.process('source')
    source.publish('x', 1)
    gate = world.process('gate', ['x'])
    gate.publish('gate', 1)
    middle = world.process('middle', ['x', 'gate'])
    middle.publish('m', 1)
    world.process('original n', ['m']).publish('n', 1)
    for key in ('a', 'c'):
        source.publish(key, 1)

    source.publish('x', 2)
    gate.publish('gate', 2)
    if target != 2:
        source.publish('x', target)
    peer = world.process('peer', ['x'])
    peer.publish('b', 1)
    source.publish('x', target + 1)
    # N restarts with a different input chain, retaining its value. M's real
    # input cut stays at X@2 because gate@2 still consumes X@2.
    world.process('restarted n', ['x']).publish('n')
    for key in ('a', 'c'):
        world.process(key, ['n']).publish(key, None if counts else 2)
    reader = world.process('reader', order)
    return world, source, middle, peer, reader


class HistoricalConflictWatchTests(unittest.TestCase):
    def test_historical_path_notification_advances_waiting_roots(self):
        for order, counts, omission in product(permutations(('a', 'c', 'b')), (False, True), (False, True)):
            with self.subTest(order=order, counts=counts, omission=omission):
                world, _, middle, _, reader = waiting_reader(order, counts)
                self.assertEqual(reader.cut(), {'a': ('a', 1, 0), 'c': ('c', 1, 0), 'b': ('b', 1, 0)})
                self.assertEqual(middle.cut()['x'], ('x', 2, 0))
                # The shared-cache image changes before every manager receives
                # its notification; no private retry or root republication.
                publisher = world.process('m without inputs') if omission else middle
                publisher.publish('m')
                expected = (1, 1) if counts else (2, 0)
                self.assertEqual(reader.cut(), {'a': ('a', *expected), 'c': ('c', *expected), 'b': ('b', 1, 0)})
                for key in ('a', 'c'):
                    choices = reader.subscriptions[key].current_dependency_revisions
                    self.assertEqual(choices[('n', 1)], ('n', 1, 0))
                    self.assertEqual(choices[('m', 1)], ('m', 1, 1))
                self.assertEqual(reader.session._subscriptions._conflicts, {})
                with patch.object(reader.session._subscriptions, '_try_advance') as retry:
                    publisher.publish('m')  # Duplicate notification from the harness.
                retry.assert_not_called()

    def test_path_remains_watched_when_both_current_and_latest_omit_it(self):
        world, _, middle, _, reader = waiting_reader()
        # Latest roots now refer directly to X@3. A/C@2 remain admissible
        # forward candidates, but N and M appear in neither endpoint tree.
        for key in ('a', 'c'):
            world.process(f'direct {key}', ['x']).publish(key, 300)
        middle.publish('m')
        self.assertEqual(reader.cut(), {'a': ('a', 2, 0), 'c': ('c', 2, 0), 'b': ('b', 1, 0)})
        self.assertEqual(reader.nodes()['x'], 2)

    def test_historical_watch_retires_after_candidate_is_below_current(self):
        world, source, middle, peer, reader = waiting_reader()
        producers = [world.process(f'direct {key}', ['x']) for key in ('a', 'c')]
        for key, producer in zip(('a', 'c'), producers):
            producer.publish(key, 300)
        middle.publish('m')
        peer.publish('b', 500)
        self.assertEqual(reader.cut(), {'a': ('a', 3, 0), 'c': ('c', 3, 0), 'b': ('b', 2, 0)})
        source.publish('x', 11)
        for key, producer in zip(('a', 'c'), producers):
            producer.publish(key, 400)
        before = reader.cut()
        with patch.object(reader.session._subscriptions, '_try_advance') as retry:
            world.process('retired m').publish('m')
        retry.assert_not_called()
        self.assertEqual(reader.cut(), before)

    def test_delayed_revision_keeps_watch_after_successor_notification(self):
        _, _, middle, _, reader = waiting_reader()
        queued = []
        with patch.object(reader.session, 'apply_update', side_effect=queued.append):
            middle.publish('m')
            middle.publish('m', 200)
        self.assertEqual([update.revision for update in queued], [(1, 1), (2, 0)])
        self.assertEqual(reader.session.apply_update(queued[1]), {})
        # The new version's header captured M@1/r1, but this cache
        # has only observed r0. The older notification can still unblock it.
        changes = reader.session.apply_update(queued[0])
        self.assertEqual(set(changes), {'a', 'c'})
        self.assertEqual(reader.cut(), {'a': ('a', 2, 0), 'c': ('c', 2, 0), 'b': ('b', 1, 0)})

    def test_old_version_remains_watched_and_can_unblock_after_successor(self):
        _, _, middle, _, reader = waiting_reader()
        middle.publish('m', 200)
        manager = reader.session._subscriptions
        self.assertIn(('m', 1), manager._conflicts)
        self.assertTrue(any(('m', 1) in versions for versions in manager._historical_conflicts.values()))
        middle.publish('m', version=1)
        self.assertEqual(reader.cut(), {'a': ('a', 2, 0), 'c': ('c', 2, 0), 'b': ('b', 1, 0)})
        self.assertEqual(reader.cache.latest('m'), ('m', 2, 0))
        self.assertEqual(reader.cache.get_latest_revision(('m', 1)), ('m', 1, 1))
        self.assertEqual(manager._historical_conflicts, {})

    def test_blocked_retry_ignores_duplicates_but_wakes_on_the_next_revision(self):
        world, _, middle, _, reader = waiting_reader(target=3)
        before = reader.cut()
        with patch.object(reader.session, 'apply_update', wraps=reader.session.apply_update) as receive:
            middle.publish('m')  # X@2 still cannot match B's X@3.
        update = receive.call_args.args[0]
        self.assertEqual(reader.cut(), before)
        details = reader.cache.get_revision(('m', 1, 0))
        older = DataUpdateDetails(id='m', revision=(1, 0), dependencies=details.dependencies,
                                  weak_dependencies=details.weak_dependencies,
                                  as_of=details.as_of)
        with patch.object(reader.session._subscriptions, '_try_advance') as retry:
            self.assertEqual(reader.session.apply_update(update), {})
            self.assertEqual(reader.session.apply_update(older), {})
        retry.assert_not_called()
        # B still consumes X@3; the gate producer's actual cut therefore
        # selects X@3 too, even though the source has already published X@4.
        world.process('later gate', ['x', 'b']).publish('gate', 300)
        middle.publish('m')
        self.assertEqual(reader.cut(), {'a': ('a', 2, 0), 'c': ('c', 2, 0), 'b': ('b', 1, 0)})
        self.assertEqual(reader.nodes()['x'], 3)

    def test_dependency_removal_uses_watch_count_before_retiring_resolved_paths(self):
        world, _, _, _, reader = waiting_reader()
        publisher = world.process('m without dependencies')
        queued = []
        with patch.object(reader.session, 'apply_update', side_effect=queued.append):
            publisher.publish('m')  # M@1/r1 removes X and G.
            publisher.publish('m', 200)
        reader.session.apply_update(queued[1])
        manager = reader.session._subscriptions
        owners = {root[0] for root, versions in manager._historical_conflicts.items() if ('m', 1) in versions}
        self.assertEqual(owners, {'a', 'c'})
        retry = manager._try_advance
        observed = []

        def check_before_retirement(sub):
            # Eligibility was captured before the new revision removed the
            # dependency. Retire paths only after the retry has run.
            observed.append(sub.data_id)
            return retry(sub)

        with patch.object(manager, '_try_advance', side_effect=check_before_retirement):
            self.assertEqual(set(reader.session.apply_update(queued[0])), {'a', 'c'})
        self.assertTrue(observed)
        self.assertEqual(reader.cut(), {'a': ('a', 2, 0), 'c': ('c', 2, 0), 'b': ('b', 1, 0)})
        self.assertEqual(manager._historical_conflicts, {})
        self.assertEqual(manager._conflicts, {})

    def test_removing_conflict_endpoint_keeps_the_consumers_notification_eligible(self):
        world, _, _, _, reader = waiting_reader()
        manager = reader.session._subscriptions
        self.assertEqual(manager._dependents[('x', 3)], {'a', 'c'})
        retry = manager._try_advance
        observed = []

        def check_after_index_refresh(sub):
            # N's update removes both paths to X@3. The gate uses N's consumer
            # watch, not the remaining number of paths to the conflict endpoint.
            self.assertNotIn(('x', 3), manager._dependents)
            self.assertEqual(manager._dependents[('n', 1)], {'a', 'c'})
            observed.append(sub.data_id)
            return retry(sub)

        with patch.object(manager, '_try_advance', side_effect=check_after_index_refresh):
            world.process('n without dependencies').publish('n')
        self.assertTrue(observed)
        self.assertEqual(reader.cut(), {'a': ('a', 2, 0), 'c': ('c', 2, 0), 'b': ('b', 1, 0)})


if __name__ == '__main__':
    unittest.main()
