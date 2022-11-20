import logging
import os
import json
conn_config = json.load(open('./config/data_directories.json'))
os.environ['KAGGLE_CONFIG_DIR'] =conn_config['kaggle_config_dir'] #TODO fujtajchl solve this global variable for kaggle

class Downloader():
    def __init__(self):
        from pathlib import Path
        conn_config = json.load(open('./config/data_directories.json'))
        #os.environ['KAGGLE_CONFIG_DIR'] = "./config" #conn_config['kaggle_config_dir']
        self.data_dir = conn_config['data_dir']
        self.data_dir_landing_raw = conn_config['data_dir_landing_raw']
        self.data_dir_landing_unzip = conn_config['data_dir_landing_unzip']

        Path(self.data_dir).mkdir(parents=True, exist_ok=True)
        Path(self.data_dir_landing_raw).mkdir(parents=True, exist_ok=True)
        Path(self.data_dir_landing_unzip).mkdir(parents=True, exist_ok=True)


    def _landing_raw_to_unzip(self, file, path_in, path_out):
        import glob
        extension = '.zip'
        if glob.glob(f'{path_in}/{file}{extension}'):
            self._unzip(f'{file}{extension}', path_in, path_out)
        else:
            from shutil import copyfile
            copyfile(path_in + '/' + file, path_out + '/' + file)

    @staticmethod
    def _unzip(fname, path_in, path_out):
        from zipfile import ZipFile, BadZipFile
        basename, extension = os.path.splitext(fname)
        try:
            with ZipFile(f"{path_in}/{fname}") as z:
                z.extract(basename,path = path_out)
        except BadZipFile as e:
            raise ValueError(
                'Bad zip file, please report on '
                'www.github.com/kaggle/kaggle-api', e)

    @staticmethod
    def _md5(fname):
        import hashlib
        hash_md5 = hashlib.md5()
        with open(fname, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def _landed_file_up_to_date(self, hash_file, file_to_check_landing_unzip):
        if hash_file == None:
            return False
        basename, extension = os.path.splitext(file_to_check_landing_unzip)
        if extension == '.zip':
            file_to_check_landing_unzip = basename
        try:
            file_to_check_md5 = self._md5(f"{self.data_dir_landing_unzip}/{file_to_check_landing_unzip}")
            lines = open(f"{self.data_dir_landing_raw}/{hash_file}")
        except FileNotFoundError:
            return False
        if len([line for line in lines if ((file_to_check_landing_unzip in line) and (file_to_check_md5 in line))]) == 1:
            return True
        return False

    def download_from_kaggle(self, kaggle_dataset, file, hash_file):
        from kaggle.api.kaggle_api_extended import KaggleApi
        api = KaggleApi()
        api.authenticate()

        #since the dataset is updated very rarery, we first check if the previously downloaded files have the same md5 hash as freshly download md5s from Kaggle
        #this not only prevents us to download the files again for no reason, but also neatly checks data integrity of downloaded and unzipped files
        #if storage was a concern (oved data validity), one could definetly load the files without unzipping (pandas.read_csv() can read zipped csv, too)
        #also various backups and file versionings could be put in place (I love such scripts so much, they have saved my a** so many times!), but let's save those for projects beyond static Kaggle dataset
       
        if (not self._landed_file_up_to_date(hash_file, file)):
            logging.info(f"Downloading {file}")
            api.dataset_download_file(kaggle_dataset, file_name=f"{file}",
                                      path=self.data_dir_landing_raw, force=True)
            self._landing_raw_to_unzip(f"{file}", f"{self.data_dir_landing_raw}",
                  f"{self.data_dir_landing_unzip}")
            if (hash_file and not self._landed_file_up_to_date(hash_file, file)):  # check again for validity
                logging.error(f"Hash invalid for {file} after downloading.")
        else:
            logging.info(f"{file} already valid and up to date in landing/unzip folder, skipping")
