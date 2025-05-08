'''Organize collections of NWP files.
'''

import tempfile, shutil, dask
from datetime import datetime
from humanize import naturalsize
import itertools as it
import numpy as np
from dask.distributed import get_client
from herbie import Herbie, wgrib2
from .nwppath import NwpPath
from .nwpdownloader import NwpDownloader

def chunk_list(l, n):
    # https://stackoverflow.com/a/312464/5548959
    return [ l[i:i + n] for i in range(0, len(l), n) ]

class NwpCollection:
    '''Manage and download a (potentially large) collection of NWP files.

    This is similar to `FastHerbie`, but includes more download optimizations
    and works with Dask. Arguments are generally the same as `Herbie`,
    `FastHerbie`, and `wgrib2.region` from Herbie.

    Args:
        DATES : pandas-parsable datetime string or list of datetimes
        fxx : int or pandas-parsable timedelta (e.g. "6h")
            Forecast lead time *in hours*. Available lead times depend on
            the model type and model version.
        model : {'hrrr', 'hrrrak', 'rap', 'gfs', 'ecmwf', etc.}
            Model name as defined in the models template folder.
            CASE INSENSITIVE; e.g., "HRRR" is the same as "hrrr".
        product : {'sfc', 'prs', 'nat', 'subh', etc.}
            Output variable product file type. If not specified, will
            use first product in model template file. CASE SENSITIVE.
            For example, the HRRR model has these products:
            - ``'sfc'`` surface fields
            - ``'prs'`` pressure fields
            - ``'nat'`` native fields
            - ``'subh'`` subhourly fields
        search : str
            If None, download the full file. Else, use regex to subset
            the file by specific variables and levels.
            Read more in the user guide:
            https://herbie.readthedocs.io/en/latest/user_guide/tutorial/search.html
        member : None or int
            Some ensemble models (e.g. the future RRFS) will need to
            specify an ensemble member.
        save_dir : str or pathlib.Path
            Location to save GRIB2 files locally. When downloading with a dask
            cluster, this must be a directory accessible within the workers.
        extent : 4-item tuple or list
            Longitude and Latitude bounds representing the region of
            interest.
            (lon_min, lon_max, lat_min, lat_max) : float

    '''
    
    def __init__(self, DATES, fxx, model, product, search, members=None,
                 save_dir=None, extent=None):
        '''Create an `NwpCollection`.
        '''
        self.DATES = DATES
        self.model = model
        self.product = product
        self.search_string = search
        self.fxx = fxx
        self.members = members
        self.save_dir = save_dir
        self.extent = extent

    def get_status(self):
        '''Print the status of the collection.
        '''
        n_files = len(self.DATES) * len(self.fxx) * len(self.members)
        full_download_size = self.file_size * n_files
        print(f'Complete download size (approximate): {naturalsize(full_download_size)}')
        if self.extent is not None:
            print('(Size on disk will be smaller due to regional subsetting.)')
        # make a matrix representing the download status of each file. 0 is
        # missing and 1 is downloaded
        download_status = np.zeros((len(self.DATES), len(self.fxx),
                                    len(self.members)), dtype=int)
        for i, date in enumerate(self.DATES):
            for j, fxx in enumerate(self.fxx):
                for k, member in enumerate(self.members):
                    if self._file_exists(date, fxx=fxx, member=member):
                        download_status[i, j, k] = 1
        n_remaining = n_files - download_status.sum()
        remaining_download_size = self.file_size * n_remaining
        print(f'{n_files - n_remaining} of {n_files} files downloaded')
        print(f'Remaining download size (approximate): {naturalsize(remaining_download_size)}')
        out = {'n_files': n_files,
               'n_remaining': n_remaining,
               'remaining_download_size': remaining_download_size,
               'download_array': download_status}
        return out

    def download(self, overwrite=False):
        '''Download all remaining files in the collection.
        '''
        status = self.get_status()
        if not status['n_remaining']:
            print('Nothing to download.')
            return dask.compute()
        # in the future it would be nice to check to see if the file exists
        # remaining_coords = np.stack(np.where(~status['download_array'])).T
        if len(status['download_array'].shape) == 2:
            coords_iter = it.product(
                range(status['download_array'].shape[0]),
                range(status['download_array'].shape[1])
            )
        elif len(status['download_array'].shape) == 3:
            coords_iter = it.product(
                range(status['download_array'].shape[0]),
                range(status['download_array'].shape[1]),
                range(status['download_array'].shape[2])
            )
        client = get_client()
        total_threads = sum(client.nthreads().values())
        # if there are many threads, combine downloads into chunks to reduce the work of
        # the scheduler
        chunk_size = 1 + total_threads // 100
        batch_size = 5000 * chunk_size
        n_batches = int(np.ceil(status['n_remaining'] / batch_size))
        self.batch_idx = 1
        def submit_batch():
            out = self._submit_download_batch(client, coords_iter, batch_size,
                                              self.batch_idx, n_batches,
                                              chunk_size)
            self.batch_idx += 1
            return out
        print(f'Dashboard link: {client.dashboard_link}')
        start_time = datetime.now()
        futures_list = []
        # get things started
        futures_list.append(submit_batch())
        futures_list.append(submit_batch())
        while len(futures_list):
            # wait for first batch of futures to complete
            client.gather(futures_list[0])
            futures_list.pop(0)
            # then submit a new batch to the list
            batch_futures = submit_batch()
            if len(batch_futures):
                futures_list.append(batch_futures)
        time_elapsed = datetime.now() - start_time
        print(f'Total run time: {time_elapsed}')

    def _submit_download_batch(self, client, coords_iter, batch_size, batch_idx,
                               n_batches, chunk_size):
        coords_batch = list(it.islice(coords_iter, batch_size))
        if chunk_size == 1:
            key = f'batch{batch_idx}/{n_batches}_download'
            return client.map(self._download_and_extract, coords_batch, key=key,
                              # decrement the priority with each batch so they
                              # complete in order
                              retries=2, priority=-batch_idx, pure=False)
        coords_lists = chunk_list(coords_batch, chunk_size)
        key = f'batch{batch_idx}/{n_batches}_download_x{chunk_size}'
        return client.map(self._download_multiple, coords_lists,
                          # decrement the priority with each batch so they
                          # complete in order
                          key=key, retries=2, priority=-batch_idx, pure=False)

    def _download_multiple(self, coords_list):
        for coords in coords_list:
            self._download_and_extract(coords)

    def _download_and_extract(self, coords):
        '''Download a single file using the coordinates from the download
        status matrix.
        '''
        date = self.DATES[coords[0]]
        fxx = self.fxx[coords[1]]
        member = self.members[coords[2]]
        nwp_file = NwpPath(date, model=self.model, product=self.product,
                           save_dir=self.save_dir, fxx=fxx, member=member)
        out_path = nwp_file.get_localFilePath()
        # create an individual directory for each download
        with tempfile.TemporaryDirectory() as tmp_dir:
            full_file = self._download(tmp_dir, date, fxx=fxx, member=member)
            region_file = wgrib2.region(full_file, self.extent)
            if not out_path.parent.is_dir():
                out_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(region_file, out_path)
            shutil.move(str(region_file) + '.idx', str(out_path) + '.idx')
        return out_path

    def _download(self, tmp_dir, *args, **kwargs):
        '''Download an NWP grib2 file using Herbie.
        '''
        tmp_archive = NwpDownloader(*args, **kwargs, save_dir=tmp_dir,
                                    model=self.model, product=self.product,
                                    verbose=False)
        # herbie will drive me insane with messages if I don't create this
        # directory
        tmp_path = tmp_archive.get_localFilePath()
        tmp_path.parent.mkdir(parents=True, exist_ok=True)
        full_file = tmp_archive.download(self.search_string)
        return full_file

    def _file_exists(self, *args, **kwargs):
        '''Check if the file exists for a given date, fxx, and member.
        '''
        nwp_file = NwpPath(*args, **kwargs, model=self.model,
                           product=self.product, save_dir=self.save_dir)
        return nwp_file.get_localFilePath().exists()

    @property
    def file_size(self):
        '''Use the inventory to calculate the file size.
        '''
        # this temp directory prevents Herbie from reading from local index
        # files, which may not match the source file
        with tempfile.TemporaryDirectory() as tmp_dir:
            h = Herbie(self.DATES[0], model=self.model, product=self.product,
                       fxx=self.fxx[0], member=self.members[0],
                       save_dir=tmp_dir, verbose=False)
        inv = h.inventory(search=self.search_string)
        return (inv['end_byte'] - inv['start_byte'] + 1).sum()

    def collection_size(self, humanize=True):
        '''Calculate the size of the collection.
        '''
        n_files = len(self.DATES) * len(self.fxx) * len(self.members)
        full_download_size = self.file_size * n_files
        if humanize:
            return naturalsize(full_download_size)
        else:
            return full_download_size
