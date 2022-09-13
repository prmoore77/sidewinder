import os
import psutil


# taken from: https://donghao.org/2022/01/20/how-to-get-the-number-of-cpu-cores-inside-a-container/
def get_cpu_count():
    if os.path.isfile('/sys/fs/cgroup/memory/memory.limit_in_bytes'):
        with open("/sys/fs/cgroup/cpu/cpu.cfs_quota_us") as fp:
            cfs_quota_us = int(fp.read())
        with open("/sys/fs/cgroup/cpu/cpu.cfs_period_us") as fp:
            cfs_period_us = int(fp.read())
        container_cpus = cfs_quota_us // cfs_period_us
        # For physical machine, the `cfs_quota_us` could be '-1'
        cpus = os.cpu_count() if container_cpus < 1 else container_cpus
    else:
        cpus = os.cpu_count()

    return cpus


def get_memory_limit():
    if os.path.isfile('/sys/fs/cgroup/memory/memory.limit_in_bytes'):
        with open('/sys/fs/cgroup/memory/memory.limit_in_bytes') as limit:
            memory_limit = int(limit.read())
    else:
        memory_limit = psutil.virtual_memory().total

    return memory_limit