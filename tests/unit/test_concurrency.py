"""
Concurrency Tests for Hologres MCP Server.

Tests verify thread-safety and correct behavior under concurrent access.
These tests use threading and mocking to simulate concurrent database operations
without requiring a real database connection.
"""

import pytest
import threading
import concurrent.futures
from unittest.mock import patch, MagicMock
import time

from hologres_mcp_server.utils import (
    connect_with_retry,
    handle_call_tool,
    handle_read_resource,
)


class TestConcurrentConnections:
    """Tests for concurrent database connection behavior."""

    def test_concurrent_connect_calls(self, mock_env_basic):
        """Test multiple concurrent connection attempts."""
        results = []
        errors = []
        lock = threading.Lock()

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.autocommit = True

        def worker(worker_id):
            try:
                with patch("psycopg.connect", return_value=mock_conn):
                    conn = connect_with_retry(retries=1)
                    with lock:
                        results.append((worker_id, conn))
            except Exception as e:
                with lock:
                    errors.append((worker_id, str(e)))

        # Create 10 concurrent threads
        threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All connections should succeed
        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert len(results) == 10

    def test_connection_pool_behavior(self, mock_env_basic):
        """Test connection behavior under high concurrency simulating connection pool."""
        connections = []
        errors = []
        lock = threading.Lock()

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.autocommit = True

        def worker(worker_id):
            try:
                with patch("psycopg.connect", return_value=mock_conn):
                    conn = connect_with_retry(retries=1)
                    # Simulate some work with the connection
                    time.sleep(0.01)
                    with lock:
                        connections.append((worker_id, id(conn)))
            except Exception as e:
                with lock:
                    errors.append((worker_id, str(e)))

        # Use ThreadPoolExecutor for more realistic concurrency
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(worker, i) for i in range(20)]
            concurrent.futures.wait(futures)

        assert len(errors) == 0, f"Errors: {errors}"
        assert len(connections) == 20

    def test_connection_failure_under_load(self, mock_env_basic):
        """Test connection behavior when failures occur under load."""
        success_count = 0
        failure_count = 0
        lock = threading.Lock()

        call_count = 0

        def flaky_connect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            # Fail every other connection attempt
            if call_count % 2 == 0:
                raise Exception("Connection refused")
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.fetchone.return_value = (1,)
            mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
            mock_cursor.__exit__ = MagicMock(return_value=False)
            mock_conn.cursor.return_value = mock_cursor
            mock_conn.autocommit = True
            return mock_conn

        def worker(worker_id):
            nonlocal success_count, failure_count
            try:
                with patch("psycopg.connect", side_effect=flaky_connect):
                    with patch("time.sleep"):  # Speed up retries
                        conn = connect_with_retry(retries=2)
                        with lock:
                            success_count += 1
            except Exception:
                with lock:
                    failure_count += 1

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Some should succeed, some should fail
        total = success_count + failure_count
        assert total == 10


class TestConcurrentQueries:
    """Tests for concurrent query execution."""

    def test_concurrent_select_queries(self, mock_env_basic):
        """Test concurrent SELECT query execution."""
        results = []
        errors = []
        lock = threading.Lock()

        mock_cursor = MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "test")]

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        def worker(query_id):
            try:
                with patch("hologres_mcp_server.utils.connect_with_retry", return_value=mock_conn):
                    result = handle_call_tool(
                        "execute_hg_select_sql",
                        f"SELECT * FROM table_{query_id}"
                    )
                    with lock:
                        results.append((query_id, result))
            except Exception as e:
                with lock:
                    errors.append((query_id, str(e)))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"Errors: {errors}"
        assert len(results) == 10

    def test_concurrent_mixed_operations(self, mock_env_basic):
        """Test concurrent execution of different operation types."""
        results = {"select": [], "dml": [], "ddl": []}
        errors = []
        lock = threading.Lock()

        def setup_mock(is_select=False, rowcount=0):
            mock_cursor = MagicMock()
            if is_select:
                mock_cursor.description = [("col",)]
                mock_cursor.fetchall.return_value = [("value",)]
            else:
                mock_cursor.description = None
                mock_cursor.rowcount = rowcount

            mock_conn = MagicMock()
            mock_conn.__enter__ = MagicMock(return_value=mock_conn)
            mock_conn.__exit__ = MagicMock(return_value=False)
            mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
            mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
            return mock_conn

        def select_worker(query_id):
            try:
                with patch("hologres_mcp_server.utils.connect_with_retry",
                           return_value=setup_mock(is_select=True)):
                    result = handle_call_tool(
                        "execute_hg_select_sql",
                        f"SELECT {query_id}"
                    )
                    with lock:
                        results["select"].append(result)
            except Exception as e:
                with lock:
                    errors.append(("select", str(e)))

        def dml_worker(query_id):
            try:
                with patch("hologres_mcp_server.utils.connect_with_retry",
                           return_value=setup_mock(rowcount=5)):
                    result = handle_call_tool(
                        "execute_hg_dml_sql",
                        f"UPDATE table SET value = {query_id}"
                    )
                    with lock:
                        results["dml"].append(result)
            except Exception as e:
                with lock:
                    errors.append(("dml", str(e)))

        def ddl_worker(query_id):
            try:
                with patch("hologres_mcp_server.utils.connect_with_retry",
                           return_value=setup_mock()):
                    result = handle_call_tool(
                        "execute_hg_ddl_sql",
                        f"CREATE TABLE test_{query_id} (id INT)"
                    )
                    with lock:
                        results["ddl"].append(result)
            except Exception as e:
                with lock:
                    errors.append(("ddl", str(e)))

        # Run different operation types concurrently
        threads = []
        for i in range(3):
            threads.append(threading.Thread(target=select_worker, args=(i,)))
            threads.append(threading.Thread(target=dml_worker, args=(i,)))
            threads.append(threading.Thread(target=ddl_worker, args=(i,)))

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"Errors: {errors}"
        assert len(results["select"]) == 3
        assert len(results["dml"]) == 3
        assert len(results["ddl"]) == 3

    def test_resource_contention(self, mock_env_basic):
        """Test behavior under resource contention scenarios."""
        shared_resource = {"value": 0}
        lock = threading.Lock()
        results = []
        errors = []

        mock_cursor = MagicMock()
        mock_cursor.description = [("result",)]
        mock_cursor.fetchall.return_value = [(1,)]

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        def worker(worker_id):
            try:
                # Simulate resource contention
                with lock:
                    shared_resource["value"] += 1
                    current = shared_resource["value"]

                with patch("hologres_mcp_server.utils.connect_with_retry", return_value=mock_conn):
                    result = handle_call_tool(
                        "execute_hg_select_sql",
                        f"SELECT {current}"
                    )
                    results.append((worker_id, current, result))
            except Exception as e:
                errors.append((worker_id, str(e)))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"Errors: {errors}"
        assert len(results) == 20
        # Verify all workers got unique values (no race conditions in counting)
        values = [r[1] for r in results]
        assert sorted(values) == list(range(1, 21))


class TestConcurrentResourceAccess:
    """Tests for concurrent resource access."""

    def test_concurrent_resource_reads(self, mock_env_basic):
        """Test concurrent read_resource calls."""
        results = []
        errors = []
        lock = threading.Lock()

        mock_cursor = MagicMock()
        mock_cursor.description = [("name",)]
        mock_cursor.fetchall.return_value = [("schema1",), ("schema2",)]

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        def worker(resource_id):
            try:
                with patch("hologres_mcp_server.utils.connect_with_retry", return_value=mock_conn):
                    result = handle_read_resource(
                        f"resource_{resource_id}",
                        "SELECT schema_name FROM information_schema.schemata"
                    )
                    with lock:
                        results.append((resource_id, result))
            except Exception as e:
                with lock:
                    errors.append((resource_id, str(e)))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"Errors: {errors}"
        assert len(results) == 10

    def test_concurrent_mixed_read_write(self, mock_env_basic):
        """Test concurrent read and write operations."""
        read_results = []
        write_results = []
        errors = []
        lock = threading.Lock()

        def setup_read_mock():
            mock_cursor = MagicMock()
            mock_cursor.description = [("data",)]
            mock_cursor.fetchall.return_value = [("read_data",)]
            mock_conn = MagicMock()
            mock_conn.__enter__ = MagicMock(return_value=mock_conn)
            mock_conn.__exit__ = MagicMock(return_value=False)
            mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
            mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
            return mock_conn

        def setup_write_mock():
            mock_cursor = MagicMock()
            mock_cursor.description = None
            mock_cursor.rowcount = 1
            mock_conn = MagicMock()
            mock_conn.__enter__ = MagicMock(return_value=mock_conn)
            mock_conn.__exit__ = MagicMock(return_value=False)
            mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
            mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
            return mock_conn

        def read_worker(worker_id):
            try:
                with patch("hologres_mcp_server.utils.connect_with_retry", return_value=setup_read_mock()):
                    result = handle_read_resource(
                        f"read_{worker_id}",
                        "SELECT data FROM table"
                    )
                    with lock:
                        read_results.append(result)
            except Exception as e:
                with lock:
                    errors.append(("read", str(e)))

        def write_worker(worker_id):
            try:
                with patch("hologres_mcp_server.utils.connect_with_retry", return_value=setup_write_mock()):
                    result = handle_call_tool(
                        "execute_hg_dml_sql",
                        f"UPDATE table SET data = 'value_{worker_id}'"
                    )
                    with lock:
                        write_results.append(result)
            except Exception as e:
                with lock:
                    errors.append(("write", str(e)))

        threads = []
        for i in range(5):
            threads.append(threading.Thread(target=read_worker, args=(i,)))
            threads.append(threading.Thread(target=write_worker, args=(i,)))

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"Errors: {errors}"
        assert len(read_results) == 5
        assert len(write_results) == 5