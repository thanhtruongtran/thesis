import re

import nltk
import spacy
from langdetect import detect
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize

from src.constants.text_processing import TextProcessingConstant

nltk.download("punkt_tab")
nltk.download("punkt")
nltk.download("wordnet")
nltk.download("stopwords")
nlp = spacy.load("en_core_web_sm")


def detect_language_one_text(text: str):
    """
    Detect language of one tweet
    input: tweet: str
    output: language: str
    """
    try:
        return detect(text)
    except Exception:
        return "unknown"


def remove_special_characters(text):
    # Remove non-alphanumeric characters
    if not isinstance(text, str):
        # Log or handle cases where input is not a string
        return ""  # Return an empty string or a default value
    text = re.sub(r"[^a-zA-Z0-9\s]", "", text)
    return text


def to_lower(text):
    # Convert all text to lowercase
    if not isinstance(text, str):
        # Log or handle cases where input is not a string
        return ""  # Return an empty string or a default value
    text = text.lower()
    return text


def tokenize(text):
    # Tokenize the text into words
    if not isinstance(text, str):
        # Log or handle cases where input is not a string
        return ""  # Return an empty string or a default value
    tokens = word_tokenize(text)
    return tokens


def stem_words(text):
    if not isinstance(text, str):
        # Log or handle cases where input is not a string
        return ""  # Return an empty string or a default value
    ps = PorterStemmer()
    # Tokenize the text into words
    words = tokenize(text)

    stop_words = set(stopwords.words("english"))
    # Stem the words and remove stopwords
    stemmed_words = [ps.stem(word) for word in words if word.lower() not in stop_words]

    return " ".join(stemmed_words)


def lemmatization(text):
    doc = nlp(text)

    lemma_dict = {}

    lemmatized_sentence = []
    for token in doc:
        # Ignore non-word characters (punctuation, spaces, ...)
        if token.is_alpha:
            lemmatized_sentence.append(token.lemma_)
            if token.lemma_ in lemma_dict:
                lemma_dict[token.lemma_].append(token.text)
            elif token.lemma_.lower() != token.text.lower():
                lemma_dict[token.lemma_] = [token.text]
        else:
            lemmatized_sentence.append(token.text)  # same original text

    lemmatized_text = " ".join(lemmatized_sentence)

    return lemmatized_text, lemma_dict


def remove_links_tags(text):
    if not isinstance(text, str):
        return ""
    processed_tweet = re.sub(r"\s+", " ", re.sub(r"@\w+", "", text)).strip()
    return re.sub(r"@[\w]+|http\S+", "", processed_tweet)


def remove_meaningless_words(text):
    if not isinstance(text, str):
        return ""
    words = text.split()
    filtered_words = [
        word
        for word in words
        if word.lower() not in TextProcessingConstant.MEANINGLESS_WORDS
    ]
    return " ".join(filtered_words)


def data_process(
    text: str,
    remove_link_tag: bool = True,
    remove_special_chars: bool = True,
    stemming: bool = True,
    lower_case: bool = True,
    remove_meaningless: bool = True,
    lemma: bool = True,
):
    # Remove @mentions and URLs
    if remove_link_tag:
        text = remove_links_tags(text)
    # Remove special characters
    if remove_special_chars:
        text = remove_special_characters(text)
    # Remove meaningless words
    if remove_meaningless:
        text = remove_meaningless_words(text)
    # Convert to lowercase
    if lower_case:
        text = to_lower(text)
    # Lemmatization
    if lemma:
        text, dict_lemma = lemmatization(text)
    # Stem words
    if stemming:
        text = stem_words(text)

    if lemma:
        return text, dict_lemma
    return text


def contains_text_or_icon(s):
    text_pattern = re.compile(r"[a-zA-Z0-9]")  # Letters and numbers
    punctuation_pattern = re.compile(r"[.,?!]")  # Common punctuation marks
    icon_pattern = re.compile(
        r"[\U0001F300-\U0001F6FF\U0001F900-\U0001F9FF\U0001FA70-\U0001FAFF]"
    )  # Emojis

    result = (
        bool(text_pattern.search(s))
        or bool(icon_pattern.search(s))
        or bool(punctuation_pattern.search(s))
    )
    if result:
        return 1

    return 0


def remove_breakline(s):
    # Remove leading spaces and newlines
    return re.sub(r"^[ \t]*\n+", "", s)
