# Some stuff used by all other scripts
import re

def clean_string(text):
    """
    Utility function to clean a string
    TODO speed this up
    """
    # Lower and remove new lines
    text_clean = text.lower().replace('\n', ' ').replace('\r', ' ')
    # Shrink spaces
    text_clean = re.sub(r'\s+', ' ', text_clean)
    # Remove lead and trailing whitespace
    text_clean = text_clean.strip()
    return text_clean


