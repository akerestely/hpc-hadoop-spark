import logging
from pathlib import Path

def resource_path(relative_path: Path) -> Path:
    import sys
    """ Get absolute path to resource, works for dev and for PyInstaller """
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = Path(sys._MEIPASS)
    except Exception:
        base_path = Path(".")

    return base_path / relative_path

def install_java():
    from subprocess import run, PIPE
    try:
        javac_path = Configs.java_home_path / "bin" / "javac"
        result = run([str(javac_path), "-version"], stderr=PIPE)
        logging.info(f"The required JDK is available on your system: {result.stderr.decode('utf-8').rstrip()}")
    except FileNotFoundError:
        logging.info("Installing java ...")
        try:
            run([str(Configs.java_intaller_path), "/s", 'ADDLOCAL="ToolsFeature,SourceFeature"'])
        except FileNotFoundError:
            logging.error(f"Java installer missing: {Configs.java_intaller_path}")
            exit(1)

def download_file(url: str, dst_folder: Path) -> Path:
    from tqdm import tqdm
    from requests import get, ConnectionError
    from pathlib import Path

    dst_folder.mkdir(parents = True, exist_ok=True)
    local_filepath: Path = dst_folder / Path(url).name
    try:
        with open(local_filepath, 'xb') as file:
            with get(url, stream=True) as response:
                logging.info(f"Downloading '{url}' to '{dst_folder.resolve()}'")
                total_size = int(response.headers.get('Content-length', 0))
                with tqdm(total=total_size, unit = "B", unit_scale = True, ascii=True) as pbar:
                    for data in response.iter_content(chunk_size=4*1024):
                        file.write(data)
                        pbar.update(len(data))
    except FileExistsError:
        logging.info(f"Skip downloading of file '{url}' as it's already downloaded to '{dst_folder}'.")
    except ConnectionError:
        logging.error(f"Could not download '{url}'")
        raise
    return local_filepath

def extract_archive(file_path: Path, dest_folder: Path):
    import tarfile
    import zipfile
    from tqdm import tqdm

    if (dest_folder / file_path.stem).exists():
        logging.info(f"Skip extracting of file '{file_path.name}' as it's already extracted to '{dest_folder}'")
        return

    if file_path.suffix == ".zip":
        archive: ZipFile = zipfile.ZipFile(file_path)
        members = archive.infolist()
    else:
        archive: TarFile = tarfile.open(file_path)
        members = archive.getmembers()

    logging.info(f"Extracting '{file_path.name}' to '{dest_folder}'")
    for member in tqdm(members, total=len(members), unit="File"):
        archive.extract(member, dest_folder)
    
    archive.close()

def rename_spark_folder():
    spark_folder_path = next(Configs.install_folder.glob(Configs.spark_folder_template))
    logging.info(f"Renaming '{spark_folder_path}'")
    while True:
        try:
            spark_folder_path.rename(Configs.spark_folder_name)
            break
        except PermissionError:
            # antivirus sometimes blocks folder to scan it
            logging.warning("Failed to rename. Retrying in a second.")
            import time
            time.sleep(1)

def copy_from_winutils_to_hadoop():
    winutils_bin: Path = Configs.temp_folder / Configs.hadoop_winutils_folder_name / Configs.hadoop_winutils_version / "bin"
    hadoop_bin: Path = Configs.install_folder / Configs.spark_folder_name / "bin"    
    logging.info(f"Copying files from '{winutils_bin}' to '{hadoop_bin}'")
    import shutil
    for file in winutils_bin.iterdir():
        shutil.copy(file, hadoop_bin)

def update_environment() -> str:
    import os

    environment = os.environ
    environment["SPARK_HOME"] = str(Configs.install_folder / Configs.spark_folder_name)
    environment["HADOOP_HOME"] = str(Configs.install_folder / Configs.spark_folder_name)

    spark_bin_path = str(Path(environment["SPARK_HOME"]) / "bin")
    if environment["PATH"].find(spark_bin_path) == -1:
        # prepend new stuff to path
        environment["PATH"] = spark_bin_path + ";" + environment['PATH']
    
    return environment

class Configs:
    java_home_path: Path = Path(r"C:\Progra~1\Java\jdk1.8.0_221")
    java_intaller_path: Path = resource_path(Path(r"jdk-8u221-windows-x64.exe"))
    temp_folder: Path = Path(r"temp").resolve()
    install_folder: Path = Path(".").resolve()
    spark_download_url: str = "http://mirrors.nav.ro/apache/spark/spark-3.0.0-preview2/spark-3.0.0-preview2-bin-hadoop3.2.tgz" # https://www.apache.org/dyn/closer.lua/spark/spark-3.0.1/spark-3.0.1-bin-hadoop3.2.tgz
    spark_folder_template: str = "spark*"
    spark_folder_name: str = "spark"
    hadoop_winutils_url: str = "https://github.com/cdarlint/winutils/archive/master.zip"
    hadoop_winutils_folder_name: str = "winutils-master"
    hadoop_winutils_version: str = "hadoop-3.2.1"   # folder name

def main():
    from argparse import ArgumentParser, SUPPRESS
    from subprocess import run
    
    # parse command line arguments
    ap = ArgumentParser()
    ap.add_argument("runas", nargs="?", choices=["install", "master", "worker"], 
        help="Run as master, worker or simply just install (default: %(default)s)", default="install")
    ap.add_argument("-a", "--address", help="Specify master address when running as worker, in the format 'hostname:port'")
    ap.add_argument("-v", "--verbosity", choices=["debug", "info", "warning", "error", "critical"], help="Log level")
    ap.add_argument("-b", "--bundle", action="store_true", help=SUPPRESS)   # hidden argument, should be known only to developer of this script
    args = ap.parse_args()
    if args.runas == "worker" and args.address is None:
        ap.error("Running as worker requires setting the address of the master")

    # setup logger
    logging.basicConfig(format="%(levelname)s: %(message)s")
    if args.verbosity is not None:
        # set user specified log level
        logging.getLogger().setLevel(getattr(logging, args.verbosity.upper()))
    else:
        # set default log level
        if args.runas == "install":
            logging.getLogger().setLevel(logging.INFO)
        else:
            logging.getLogger().setLevel(logging.WARNING)

    # bundle and exist when requested
    if args.bundle:
        logging.getLogger().setLevel(logging.INFO)
        logging.info("Bundling application")
        run('pyinstaller -y -F --add-data "jdk-8u221-windows-x64.exe";"." --uac-admin "install.py"')
        # remove leftovers
        import shutil
        shutil.rmtree("build", True)
        Path("install.spec").unlink()
        input("Press any key to exit ...")
        # stop program here
        exit(0)

    # install spark
    install_java()
    if not (Configs.install_folder / Configs.spark_folder_name).exists():
        spark_archive_path: Path = download_file(Configs.spark_download_url, Configs.temp_folder)
        extract_archive(spark_archive_path, Configs.install_folder)
        rename_spark_folder()

        if not (Configs.temp_folder / Configs.hadoop_winutils_folder_name).exists():
            hadoop_winutils_archive_path: Path = download_file(Configs.hadoop_winutils_url, Configs.temp_folder)
            extract_archive(hadoop_winutils_archive_path, Configs.temp_folder)
            copy_from_winutils_to_hadoop()
        else:
            logging.info(f"Skipping downloading and extracting of '{Configs.hadoop_winutils_url}' as it's already extracted")
    else:
        logging.info(f"Skipping downloading and extracting of '{Configs.spark_download_url}' as it's already extracted")

    update_environment()

    # run as requested or just open a command prompt
    try:
        if args.runas == "master":
            run(['spark-class.cmd', 'org.apache.spark.deploy.master.Master'])
        elif args.runas == "worker":
            run(['spark-class.cmd', 'org.apache.spark.deploy.worker.Worker', 'spark://' + args.address])
        else:
            run(['cmd'])
    except KeyboardInterrupt:
        pass

    # to access pyspark in jupyter notebook run:
    # conda install -c conda-forge findspark

if __name__ == "__main__":
    main()