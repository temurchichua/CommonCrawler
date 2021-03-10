import os

# common crawl storage server
from tqdm import tqdm

from tools.file_managment import gzip_to_file, save_file, search_in_file, lines_in_file
from tools.timing import Timer
from tools.wet_handler import notify, wet_line_to_text

BASE_URL = 'https://commoncrawl.s3.amazonaws.com/'
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def chunk_index(n):
    chunk = str(n)
    index = (5 - len(chunk)) * "0" + chunk
    return index


def cdx_url_generator(cdx_index, month_index):
    chunk_url = f"{BASE_URL}cc-index/collections/CC-MAIN-{month_index}/indexes/cdx-{cdx_index}.gz "
    return chunk_url


def get_cdx(cdx_url, cc_directory, log=False):
    """
    downloads cdx file and extracts text from the warc index

    Args:
        cdx_url (string): used cdx_url_generator() to generate cdx index
    """
    try:
        return gzip_to_file(cdx_url, dir_path=cc_directory, log=log)
    except Exception as e:
        notify(f"corrupted file at {cdx_url}: {e}")
        return None


def cdx_file_processor(cdx_file, pool):
    total = lines_in_file(cdx_file)
    with open(cdx_file) as cdx_file:
        # for index_line in tqdm(cdx_file, total=total, desc="Processing CDX"):
        #     cdx_line_handler(index_line, pool)
        with pool:
            for _ in tqdm(pool.imap_unordered(wet_line_to_text, cdx_file), total=total, desc="Parallel Process"):
                pass


def cdx_processor(step_index, month_index, pool):
    cdx_index = chunk_index(step_index)
    cc_directory = f'data/{month_index}'

    # if file was already processed
    if search_in_file(cdx_index, f'{cc_directory}/finished_cdx.txt'):
        tqdm.write(f"file {cdx_index} has already been processed")
        return None

    cdx_url = cdx_url_generator(cdx_index, month_index)

    # download end extract cdx file
    cdx_file = get_cdx(cdx_url, cc_directory, log=True)

    # if there was error handling the cdx archive
    if not cdx_file:
        return None

    cdx_file_processor(cdx_file, pool)

    # clean the workspace
    os.remove(cdx_file)

    # add file to finished files list
    save_file(cdx_index, f'{cc_directory}/finished_cdx.txt')

    return True


def main_loop(month_index, pool):
    for step_index in range(1, 300):
        t = Timer()
        with t:
            cdx_processor(step_index, month_index, pool)
        notify(f"- ✔ finished cdx {step_index} in {(t.elapsed_time / 60):.2f} min")


if __name__ == '__main__':
    month_index = '2021-04'
    for step_index in range(1, 300):
        t = Timer()
        with t:
            cdx_processor(step_index, month_index)
        notify(f"- ✔ finished cdx {step_index} in {(t.elapsed_time / 60):.2f} min")
