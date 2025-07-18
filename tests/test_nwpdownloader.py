
import pandas as pd
from nwpdownload.nwpdownloader import NwpDownloader

def test_download():
    '''Test that NwpDownloader correctly downloads a file.
    '''
    search_0p25 = '|'.join([
        ':TMP:2 m above ground:',
        ':DPT:2 m above ground:',
        ':PRES:surface:'
    ])
    runs = pd.date_range(start='2021-04-01 12:00', periods=4, freq='D')
    fxx = range(3, 24 * 8, 3)
    archive = NwpDownloader(runs[0], model='gefs', fxx=fxx[0],
                            product='atmos.25', member='avg',
                            save_dir='tests/data')
    data_file = archive.download(search_0p25)
    # should I check the file?
    # assert inc(3) == 5
