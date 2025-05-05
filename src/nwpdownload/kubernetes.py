'''Functions to set up a kubernetes cluster for downloading data.
'''

from dask_kubernetes.operator import KubeCluster, make_cluster_spec

def k8s_download_cluster(name, out_dir, namespace=None, n_workers=3,
                         threads_per_worker=6, worker_memory='1G',
                         local_domain=None, port_forward_cluster_ip=None):
    '''Set up a kubernetes cluster for downloading NWP data.

    Each worker has 1 CPU, and writes data to `out_dir`. When creating a
    `NwpCollection` using this cluster, set `save_dir="/mnt/nwp"`. This is where
    the data directory will be mounted within the worker containers.

    Requirements:
      - `kubectl` on the system running python, configured to connect to
        kubernetes
      - the dask-kubernetes-operator helm chart must be installed on kubernetes
      - user permission to create kubernetes pods in the selected namespace

    Args:
        name: Name of the cluster.
        out_dir: Directory where files will be downloaded. This directory will
            be mounted by the workers.
        namespace: Namespace in which to launch the workers. Defaults to current
            namespace if available or "default".
        n_workers: Number of workers.
        threads_per_worker: Threads per worker (core). For downloads, it's
            efficient to have multiple threads per core.
        worker_memory: Worker RAM.
        local_domain: The local domain used by kubernetes (the kubernetes
            default is "cluster.local"). If `None`, will attempt to find the
            domain automatically.
        port_forward_cluster_ip: Set to `True` if the main python is running
            outside of kubernetes. If `None`, will attempt to find the correct
            setting.

    Returns:
        dask cluster

    '''
    worker_resources = {'limits': {'cpu': '1', 'memory': worker_memory}}
    if local_domain is None:
        scheduler_address = f'tcp://{name}-scheduler:8786'
    else:
        scheduler_address = f'tcp://{name}-scheduler.svc.{local_domain}:8786'
    env = {'DASK_SCHEDULER_ADDRESS': scheduler_address,
           # make sure we get the newest version
           'EXTRA_PIP_PACKAGES': 'git+https://github.com/ASRCsoft/nwpdownload --upgrade'}
    spec = make_cluster_spec(name=name, n_workers=n_workers,
                             resources=worker_resources, env=env)
    image = 'wcmay/nwpdownload:firstpush'
    scheduler = spec['spec']['scheduler']['spec']['containers'][0]
    worker = spec['spec']['worker']['spec']['containers'][0]
    scheduler['image'] = image
    worker['image'] = image
    # give the scheduler a little extra time to install the package
    scheduler['livenessProbe']['initialDelaySeconds'] = 30
    # add the volume for writing data
    volume = {'hostPath': {'path': out_dir},
              'name': 'nwpout'}
    mount = {'name': 'nwpout', 'mountPath': '/mnt/nwp'}
    spec['spec']['worker']['spec']['volumes'] = [volume]
    worker['volumeMounts'] = [mount]
    worker['args'].extend(['--nthreads', str(threads_per_worker)])
    return KubeCluster(namespace=namespace, custom_cluster_spec=spec,
                       port_forward_cluster_ip=port_forward_cluster_ip)
