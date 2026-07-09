from mobilerun.orchestration.models import TaskRecord, TaskRequest, TaskStatus
from mobilerun.orchestration.queue import TaskQueue


def _insert_test_task(queue: TaskQueue, request: TaskRequest, status: TaskStatus) -> None:
    """Insert a test task into the test queue"""
    queue._store[request.id] = TaskRecord(request=request, status=status)

def test_list_no_filter():
    queue = TaskQueue()
    r1, r2, r3 = TaskRequest("bomb the battery"), TaskRequest("fly"), TaskRequest("eat your owner")
    _insert_test_task(queue, r1, TaskStatus.WAITING)
    _insert_test_task(queue, r2, TaskStatus.RUNNING)
    _insert_test_task(queue, r3, TaskStatus.WAITING)

    ids = {r.request.id for r in queue.list()}
    assert ids == {r1.id, r2.id, r3.id}


def test_list_filter():
    queue = TaskQueue()
    r1, r2, r3 = TaskRequest("bomb the battery"), TaskRequest("fly"), TaskRequest("eat your owner")
    _insert_test_task(queue, r1, TaskStatus.WAITING)
    _insert_test_task(queue, r2, TaskStatus.RUNNING)
    _insert_test_task(queue, r3, TaskStatus.WAITING)

    waiting = {r.request.id for r in queue.list(TaskStatus.WAITING)}
    running = {r.request.id for r in queue.list(TaskStatus.RUNNING)}

    assert waiting == {r1.id, r3.id}
    assert running == {r2.id}

def test_list_filter_return_empty():
    queue = TaskQueue()
    r1, r2, r3 = TaskRequest("bomb the battery"), TaskRequest("fly"), TaskRequest("eat your owner")
    _insert_test_task(queue, r1, TaskStatus.WAITING)
    _insert_test_task(queue, r2, TaskStatus.RUNNING)
    _insert_test_task(queue, r3, TaskStatus.WAITING)

    assert queue.list(TaskStatus.CANCELLED) == []

def test_cancel_only_waiting():
    queue = TaskQueue()
    r1, r2, r3 = TaskRequest("bomb the battery"), TaskRequest("fly"), TaskRequest("eat your owner")
    _insert_test_task(queue, r1, TaskStatus.WAITING)
    _insert_test_task(queue, r2, TaskStatus.RUNNING)
    _insert_test_task(queue, r3, TaskStatus.COMPLETED)

    assert queue.cancel(r1.id) is True
    assert queue.get(r1.id).status == TaskStatus.CANCELLED
    assert queue.cancel(r2.id) is False
    assert queue.cancel(r3.id) is False
    assert queue.get(r2.id).status == TaskStatus.RUNNING
    assert queue.cancel("id not exists") is False

def test_listener_can_fire():
    queue = TaskQueue()
    r1 = TaskRequest("bomb the battery")
    _insert_test_task(queue, r1, TaskStatus.WAITING)

    seen = []

    queue.add_listener(lambda record: seen.append((record.request.id, record.status)))

    queue.cancel(r1.id)

    assert seen == [(r1.id, TaskStatus.CANCELLED)]

def _raise_exception(record):
    raise Exception("test exception")

def test_listener_not_stop_transition():
    queue = TaskQueue()
    r1 = TaskRequest("bomb the battery")
    _insert_test_task(queue, r1, TaskStatus.WAITING)

    queue.add_listener(_raise_exception)

    queue.cancel(r1.id)

    assert queue.get(r1.id).status == TaskStatus.CANCELLED

def test_listener_independent():
    queue = TaskQueue()
    r1 = TaskRequest("bomb the battery")
    _insert_test_task(queue, r1, TaskStatus.WAITING)

    seen = []

    queue.add_listener(_raise_exception)
    queue.add_listener(lambda record: seen.append((record.request.id, record.status)))

    queue.cancel(r1.id)

    assert seen == [(r1.id, TaskStatus.CANCELLED)]
