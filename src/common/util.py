# Some stuff used by all other scripts
import re

def clean_string(text):
    """
    Utility function to clean a string
    """
    # Remove some extra things
    text_clean = text.replace('.', '').replace('_', ' ').lower()
    # Shrink spaces
    text_clean = re.sub(r'\s+', ' ', text_clean)
    # Remove lead and trailing whitespaces
    text_clean = text_clean.strip()
    return text_clean


