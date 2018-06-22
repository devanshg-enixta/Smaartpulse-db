import os
import re
from gensim.utils import to_unicode, smart_open, dict_from_corpus


# If we see a new sentence of a review, we filter for the super phrases
# and filter out the sub phrases if they are there.
# In case of overlapping phrases we select the higher coherence
# phrases.

def check_super_phrase(phrase_list, phrase_dict, window=16):
    """the phrases are in lowercase,
    We will iterate through phrases in the phrase_list and see
    if there is any sub-phrase in the phrase_dict after deleting
    the current phrase .It will store the new super phrases
    We will remove sub phrases if they are found

    :param phrase_list: list of valid phrases found in line
    :param phrase_dict: copy of the above list in dict form
    :param window: size of window to be considered
    :return: list of valid super phrases
    """
    #print phrase_list

    new_phrase_dict = phrase_dict.copy()

    for num_phr, phrase in enumerate(phrase_list):

        # flag variable checks for the presence of sub-phrase
        temp_phrase_dict = phrase_dict.copy()

        # deletes the current phrase from the dictionary
        del temp_phrase_dict[phrase]

        # tokenizes the current phrase with "whitespace" separator
        toks = phrase.split(" ")

        # Iterates through the toks to check for presence of sub-phrase
        # Here we will have to iterate through the toks[0:] since we also
        # need to find unigrams
        for i, start_token in enumerate(toks[0:]):

            # Since we will be having uni-grams in curated phrases,
            # the arguments to range function will be given as range(0, window)
            for j in range(0, window):
                if i + j + 1 <= len(toks):
                    new_string = " ".join(toks[i:i + j + 1])
                    if new_string in temp_phrase_dict:
                        if new_string in new_phrase_dict:
                            del new_phrase_dict[new_string]
    #print >> outfile, ""
    #print new_phrase_dict
    return new_phrase_dict

#############################################################################


def dict_of_phrases(phrase_dir):
    phrase_dict = dict()
    global_dict = dict()


    # Making a dict() for all the phrases we have
    # and ensure that the phrases don't have underscores in them

    for DIR in os.listdir(phrase_dir):
        phrase_dict[DIR] = dict()
        if DIR.startswith("."):
            continue
        for infile in os.listdir(os.path.join(phrase_dir, DIR)):
            if infile.startswith("."):
                continue
            if infile.endswith(".txt") :
                fname = infile.split(".txt")[0]
                phrase_dict[DIR][fname] = dict()

                # reading the phrases in file
                with open(phrase_dir + "/" + DIR + "/" + infile, 'rb') as foo:
                    #if "/r" in foo:
                        #foo = foo.split("/r")


                    line_num = 0
                    for line in foo:
                        if "\r" in line:
                            #print "true there is \r is here"
                            linee = line.split("\r")
                            #line = .join(line)
                            for line in linee:
                                #print line
                                line_num += 1

                                # Replacing underscores with spaces
                                line = line.decode('ascii', 'ignore').replace("_", " ").replace("\n\r", "\n")

                                # Replacing dashes with spaces so as to prevent errors
                                # in detecting word boundaries
                                line = line.replace("-", " ")
                                phrase = line.strip().split("|")[0]
                                #print phrase

                                # check for empty line
                                if phrase == "":
                                    #print line_num
                                    continue

                                if phrase not in phrase_dict:
                                    phrase_dict[DIR][fname][phrase] = 1
                                    global_dict[phrase] \
                                        = 1
                        else:
                            #print line
                            line_num += 1

                            # Replacing underscores with spaces
                            line = line.decode('ascii','ignore').replace("_", " ").replace("\n\r","\n")

                            # Replacing dashes with spaces so as to prevent errors
                            # in detecting word boundaries
                            line = line.replace("-", " ")
                            phrase = line.strip().split("|")[0]
                            #print phrase

                            # check for empty line
                            if phrase == "":
                                #print line_num
                                continue

                            if phrase not in phrase_dict:
                                phrase_dict[DIR][fname][phrase] = 1
                                #phrase_dict[sentiments][positive][good] = 1
                                #{sentiments:{'positive':{'good': 1, 'awesome': 1}}}
                                #{'good': 1, 'awesome': 1, 'bad':1}

                                global_dict[phrase]\
                                    = 1
    #print >> outfile, global_dict
    return phrase_dict, global_dict

##############################################################################


def filter_side_overlapping_phrases(line_phrase_dict, tokens):
    """This function selects phrases which can be side overlapping by their
    coherence value. Say for example, two phrases are "great President" and
    "President of India". The word "President" is common between them.
    One of the phrases is selected based on the higher value of
    phrases coherence.
    :param line_phrase_dict: dict containing valid phrases in sentence
    :param tokens: list of words in sentence
    :return: (phrasified tokens in sentence, selected phrases list, string)
    """

    # sorting the phrases in descending order of coherence
    temp_list = sorted(line_phrase_dict.items(),
                       key=lambda x: x[1],
                       reverse=True)

    # string = " ".join([x.encode('utf-8') for x in tokens])
    string = " ".join(tokens)
    # print tokens
    line_phrase_list = list()
    #print "printing string"
    #print string

    # filtering out the non-overlapping phrases in line
    # and adding uni-grams in line_phrase_list
    for phrase, _ in temp_list:
        if len(phrase.split()) is 1:
            line_phrase_list.append(phrase)
        else:
            # string has the valid phrase words joined by "_"
            a ="_".join(phrase.split())
            string = string.replace(phrase, a)
            #string = re.sub(r'\b' + phrase + r'\b', "_".join(phrase.split()), string)
            #print string

    # Tokenize the string by whitespace. SEARCH the tokens for underscores.
    # if "_" is found, REPLACE the "_" by " ", to get the desired phrase.
    toks = string.split()
    for word in toks:
        if "_" in word:
            word = word.replace("_", " ")
            word = word
            line_phrase_list.append(word)
    return toks, line_phrase_list, string

##############################################################################


def valid_phrase_search(tokens, all_phrases_dict, window):
    """It searches for all the phrases in tokens within window length
     which are present in all_phrases_dict
    :param tokens: list of words in sentence after tokenizing
    :param all_phrases_dict: dict of all the aspects and sentiments lexicons
    :param window: window size
    :return: dict with keys as phrases found in tokens
    """

    temp_dict = dict()

    #print all_phrases_dict
    # Modifying the code to incorporate uni-grams
    for i, start_token in enumerate(tokens[0:]):
        # starting the range from zero to incorporate uni-grams
        for j in range(0, window):
            if i + j + 1 <= len(tokens):
                new_string = " ".join(tokens[i:i + j + 1])
               # print new_string
                if new_string in all_phrases_dict:
                   # print new_string,'---->',all_phrases_dict[new_string]
                    temp_dict[new_string] = all_phrases_dict[new_string]

    return temp_dict

##############################################################################


def change_sentiment_polarity(sent_type):
    """The code changes the polarity of the sentiment type
    of the variable sent_type

    :param sent_type: It is the sentiment type i.e,
    one of the 5 sentiment classes
    :return: negation of the sent_type
    """
    if sent_type == "positive":
        sent_type = "negative"

    elif sent_type == "negative":
        sent_type = "positive"

    elif sent_type == "most-positive":
        sent_type = "negative"

    elif sent_type == "most-negative":
        sent_type = "positive"

    elif sent_type == "neutral":
        sent_type = "negative"

    return sent_type

def change_emotion_type(sent_type):
    """The code changes the polarity of the sentiment type
    of the variable sent_type

    :param sent_type: It is the sentiment type i.e,
    one of the 5 sentiment classes
    :return: negation of the sent_type
    """
    #Joy, trust, fear, surprise, sadness, disgust, anger, anticipation

    #joy versus sadness
    #trust versus disgust
    #fear versus anger
    #anticipation versus surprise

    if sent_type == "joy":
        sent_type = "sadness"

    elif sent_type == "sadness":
        sent_type = "joy"

    elif sent_type == "trust":
        sent_type = "disgust"

    elif sent_type == "disgust":
        sent_type = "trust"

    elif sent_type == "fear":
        sent_type = "anger"

    elif sent_type == "anger":
        sent_type = "fear"

    elif sent_type == "anticipation":
        sent_type = "surprise"

    elif sent_type == "surprise":
        sent_type = "anticipation"

    return sent_type

##############################################################################


def sentence_preprocessing(line, review_field, delimiter='~'):

    # converting the line to unicode and selecting review
    sentence = to_unicode(line).split(delimiter)[review_field]

    # By doing this words like don't and can't will become dont and cant
    sentence = sentence.replace("'", "")

    # Replacing dashes with spaces so as to prevent errors
    # in detecting word boundaries
    sentence = sentence.replace("-", " ")

    # Making the sentence lowercase and stripping whitespaces in the end
    sentence = sentence.lower().strip()

    return sentence

##############################################################################


def load_negation_words(negation_words_file):
    """ reads negation words from the "negation.sorted.txt file"
    :param negation_words_file: file containing words which induce negation
    :return: list of negation words
    """
    #neg_words_file = os.path.join(negation_words_dir, "negation.sorted.txt")

    # reading the entire file using readlines()
    neg_words_list = smart_open(negation_words_file, mode='rb').readlines()

    # striping the "/n" at the end of every line
    neg_words_list = list(word.strip() for word in neg_words_list)
    return neg_words_list

##############################################################################


