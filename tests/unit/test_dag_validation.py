"""DAG validation. A genuinely professional touch most students omit.

Airflow DAG bugs are otherwise only discovered at runtime, in production, at
2am. These checks catch them on the pull request instead.

Skipped unless Airflow is installed, so the fast unit job does not need it.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.unit

airflow = pytest.importorskip("airflow", reason="Airflow not installed in this job")


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag

    return DagBag(dag_folder="dags/", include_examples=False)


def test_no_dag_import_errors(dagbag):
    """A DAG import error shows in the UI as a small red banner and nowhere else."""
    assert not dagbag.import_errors, f"DAG import errors: {dagbag.import_errors}"


def test_every_dag_has_an_owner_and_retries(dagbag):
    for dag_id, dag in dagbag.dags.items():
        assert dag.default_args.get("owner"), f"{dag_id} has no owner"
        assert dag.default_args.get("retries") is not None, f"{dag_id} has no retries"


def test_every_dag_has_an_execution_timeout(dagbag):
    """Without one, a hung task shows 'running' all night and nobody is paged."""
    for dag_id, dag in dagbag.dags.items():
        assert dag.default_args.get("execution_timeout"), f"{dag_id} has no timeout"


def test_no_dag_has_catchup_enabled(dagbag):
    """catchup=True on first unpause backfills a year of runs at once."""
    for dag_id, dag in dagbag.dags.items():
        assert dag.catchup is False, f"{dag_id} has catchup enabled"


def test_dags_are_acyclic(dagbag):
    """The 'A' in DAG. Airflow enforces it, but assert it so the failure is readable."""
    from airflow.utils.dag_cycle_tester import check_cycle

    for dag_id, dag in dagbag.dags.items():
        check_cycle(dag)
