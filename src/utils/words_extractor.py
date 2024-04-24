import re


def words_extractor(text: str):
    text = re.split(r'\b\W+', text)
    text.remove("")
    return text

