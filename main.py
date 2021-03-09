# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from tools.timing import Timer
from tools.wet_handler import wet_line_to_text, notify

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
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

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
