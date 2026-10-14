"""Fake JVM-shaped objects for tests. py4j exposes Scala case-class fields and
zero-arg methods identically from Python's point of view: both show up as a
callable attribute returning the value. `Attr` mimics exactly that shape so
extract.py's `_get` chain-walker can be tested without a live JVM."""


class Attr:
    def __init__(self, **values):
        self._values = values

    def __getattr__(self, name):
        if name in self._values:
            value = self._values[name]
            return lambda: value
        raise AttributeError(name)


def fake_task_end(stage_id: int, stage_attempt: int, duration_ms: float) -> Attr:
    return Attr(
        stageId=stage_id,
        stageAttemptId=stage_attempt,
        taskInfo=Attr(duration=duration_ms),
    )


def fake_stage_completed(
    stage_id: int,
    stage_attempt: int,
    num_tasks: int,
    shuffle_read_bytes: int = 0,
    shuffle_write_bytes: int = 0,
    disk_spill_bytes: int = 0,
    mem_spill_bytes: int = 0,
    gc_time_ms: int = 0,
    input_bytes: int = 0,
    output_bytes: int = 0,
) -> Attr:
    task_metrics = Attr(
        shuffleReadMetrics=Attr(totalBytesRead=shuffle_read_bytes),
        shuffleWriteMetrics=Attr(bytesWritten=shuffle_write_bytes),
        diskBytesSpilled=disk_spill_bytes,
        memoryBytesSpilled=mem_spill_bytes,
        jvmGCTime=gc_time_ms,
        inputMetrics=Attr(bytesRead=input_bytes),
        outputMetrics=Attr(bytesWritten=output_bytes),
    )
    stage_info = Attr(
        stageId=stage_id,
        attemptNumber=stage_attempt,
        numTasks=num_tasks,
        taskMetrics=task_metrics,
    )
    return Attr(stageInfo=stage_info)
