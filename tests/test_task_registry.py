import pytest

from asyncmq.task import TASK_REGISTRY, task


@task(queue="default")
async def foo():
    return "bar"

@task(queue="custom")
async def add(a, b):
    return a + b

@task(queue="math")
async def multiply(x, y):
    return x * y

@task(queue="text")
async def shout(word):
    return word.upper()

@task(queue="default")
async def echo(v):
    return v

def get_task_id(func):
    for k, v in TASK_REGISTRY.items():
        if v["func"] == func:
            return k
    raise ValueError("Task not registered")

def test_task_is_registered():
    task_id = get_task_id(foo)
    assert task_id in TASK_REGISTRY
    assert TASK_REGISTRY[task_id]["queue"] == "default"

def test_multiple_task_registration():
    id1 = get_task_id(foo)
    id2 = get_task_id(add)
    assert id1 != id2
    assert TASK_REGISTRY[id1]["queue"] == "default"
    assert TASK_REGISTRY[id2]["queue"] == "custom"

def test_registered_task_is_callable():
    id = get_task_id(add)
    task_func = TASK_REGISTRY[id]["func"]
    assert callable(task_func)

def test_task_registry_keys_are_strings():
    for key in TASK_REGISTRY:
        assert isinstance(key, str)

def test_registry_values_have_expected_structure():
    for value in TASK_REGISTRY.values():
        assert "func" in value
        assert "queue" in value

@pytest.mark.asyncio
async def test_can_call_registered_multiply():
    id = get_task_id(multiply)
    result = await TASK_REGISTRY[id]["func"](2, 3)
    assert result == 6

@pytest.mark.asyncio
async def test_can_call_registered_shout():
    id = get_task_id(shout)
    result = await TASK_REGISTRY[id]["func"]("hi")
    assert result == "HI"

def test_registered_function_names():
    names = [v["func"].__name__ for v in TASK_REGISTRY.values()]
    assert "foo" in names
    assert "add" in names
    assert "multiply" in names

def test_registry_supports_different_queues():
    queues = set(v["queue"] for v in TASK_REGISTRY.values())
    assert {"default", "custom", "math", "text"}.issubset(queues)

def test_task_id_collision_does_not_occur():
    ids = list(TASK_REGISTRY.keys())
    assert len(ids) == len(set(ids))

def test_echo_function_registration():
    task_id = get_task_id(echo)
    assert task_id in TASK_REGISTRY
    assert TASK_REGISTRY[task_id]["queue"] == "default"

def test_task_id_format():
    for key in TASK_REGISTRY:
        assert "." in key  # expect module.function format

@pytest.mark.asyncio
async def test_add_is_async():
    func = TASK_REGISTRY[get_task_id(add)]["func"]
    result = await func(4, 5)
    assert result == 9

def test_all_registered_are_coroutines():
    import inspect
    for entry in TASK_REGISTRY.values():
        assert inspect.iscoroutinefunction(entry["func"])

def test_task_registry_is_not_empty():
    assert len(TASK_REGISTRY) > 0

def test_task_registration_via_decorator():
    from types import FunctionType
    keys = [k for k in TASK_REGISTRY]
    assert all(isinstance(TASK_REGISTRY[k]["func"], FunctionType) for k in keys)
