import json
import os
from json.decoder import JSONDecodeError

from tqdm import tqdm
import fasttext

from tools.file_managment import gzip_to_file, save_file, search_in_file, file_downloader
from tools.timing import Timer
from tools.slacker import post_to_slack

# common crawl storage server
BASE_URL = 'https://commoncrawl.s3.amazonaws.com/'
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class LanguageIdentification:

    def __init__(self):
        pretrained_lang_model = "models/lid.176.bin"
        model_path = os.path.join(BASE_DIR, pretrained_lang_model)

        if not os.path.exists("tools/models/"):
            os.makedirs("tools/models/")

        if not os.path.exists(model_path):
            # !!! add mkdir functionality
            download_url = "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin"
            file_downloader(download_url, os.path.dirname(model_path))

        self.model = fasttext.load_model(model_path)

    def predict_lang(self, text):
        predictions = self.model.predict(text)  # returns top matching language
        return predictions


LANGUAGE = LanguageIdentification()


def notify(message):
    tqdm.write(message)
    post_to_slack(message)


def warc_line_to_json(warc_line, for_wet=False):
    """
    :returns dict from warcs line + adds wet warcs url
    """
    try:
        warc_index = json.loads('{"url":' + warc_line.split('{"url":')[1])
    except JSONDecodeError as error:
        notify(f"error {error} at {warc_line}")
        save_file(warc_line, f'data/corrupted_warc_lines.txt')
        return None

    warc_index['warc_url'] = BASE_URL + warc_index['filename']
    warc_index['CC'] = "-".join(warc_index['filename'].split("/")[1].split("-")[2:4])
    if for_wet:
        warc_index['wet_url'] = warc_index['warc_url'].replace('/warc/', '/wet/').replace('warc.gz', 'warc.wet.gz')
    return warc_index


def get_wet(warc_index):
    """
    downloads wet file and extracts text from the warc index

    Args:
        warc_index (dict): used warc_line_to_json() to generate warc index
    """
    try:
        return gzip_to_file(warc_index['wet_url'], dir_path="data/" + warc_index['CC'])
    except Exception as e:
        tqdm.write(f"corrupted file at {warc_index['filename']}: {e}")
        save_file(warc_index['filename'], f'data/{warc_index["CC"]}/corrupted_warcs.txt')
        return None


def process_wets(file_path, language="ka", sequence=False, separator="\n"):
    """ extracts text out of WET file"""
    flag = False
    clear_list = list()
    with open(file_path, 'r', encoding="utf-8") as infile:
        for a_line in infile:
            if "WARC-Identified-Content-Language" in a_line:
                if "kat" in a_line:
                    flag = True
                    continue

                else:
                    flag = False
                    continue

            if flag:
                # process
                split_line = a_line.strip()
                if split_line != "" and split_line not in clear_list:
                    if language:
                        predicted_language = LANGUAGE.predict_lang(split_line)
                        if predicted_language[0][0] == f"__label__{language}" and predicted_language[1][0] > 0.8:
                            clear_list.append(split_line)
                        else:
                            continue
                    else:
                        clear_list.append(split_line)

        if clear_list:
            if sequence:
                return clear_list

            else:
                save_file(separator.join(clear_list))


def wet_line_to_text(wet_line):
    # get index dict from warc_line
    index = warc_line_to_json(wet_line, for_wet=True)
    if not index:
        return None

    # if wet file doesn't contain Georgian text
    if 'kat' not in index.get('languages', []):
        return None

    # if file was already processed
    if search_in_file(index['filename'], f'data/{index["CC"]}/finished_wets.txt'):
        tqdm.write(f"file {index['filename']} has already been processed")
        return None

    # download end extract wet file
    wet_file = get_wet(index)

    # if there was error handling the wet archive
    if not wet_file:
        return None

    # main wet file to text file process
    process_wets(wet_file)

    # clean the workspace
    os.remove(wet_file)

    # add file to finished files list
    save_file(index['filename'], f'data/{index["CC"]}/finished_wets.txt')

    return True


if __name__ == "__main__":
    #  parse_index_file_by_language("D:/PycharmProjects/commoncrawl/data/2021-04/cdx-00000")
    # file_url = "https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2021-04/cc-index.paths.gz"

    line = '0,123,172,167)/index.php/legales 20210117150147 {"url": "https://167.172.123.0/index.php/legales/", ' \
           '"mime": "text/html", "mime-detected": "text/html", "status": "200", "digest": ' \
           '"A7ZADSTR36UGBGR5Z5UUTX73OBHDGCVK", "length": "31415", "offset": "191660846", "filename": ' \
           '"crawl-data/CC-MAIN-2021-04/segments/1610703513062.16/warc/CC-MAIN-20210117143625-20210117173625' \
           '-00548.warc.gz", "charset": "UTF-8", "languages": "spa"} '
    t = Timer()
    with t:
        wet_line_to_text(line)
    notify(f"- ✔ finished wet in {(t.elapsed_time / 60):.2f} min")
