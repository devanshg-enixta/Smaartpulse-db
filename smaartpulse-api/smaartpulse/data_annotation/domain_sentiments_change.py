
__author__ = "Mohammed Murtuza"
__copyright__ = "Copyright 2017, Enixta Innovations"



from gensim.utils import smart_open
import os
import argparse

if __name__ == "__main__":

    parser = argparse.ArgumentParser("Change the sentiment polarity according to the domain/aspect ex.: low is positive for price, low is negative for battery")

    parser.add_argument('--infile',
                        type=str,
                        help = 'data annotation output file')

    parser.add_argument('--outfile',
                        type=str,
                        help='modified according to the domain')

    parser.add_argument('--phrase_dir',
                        type=str,
                        help='directory location domain sentiment words')

    args = parser.parse_args()
    annotated_file = args.infile
    infile = smart_open(annotated_file, 'rb')
    output_file = args.outfile
    outfile = smart_open(output_file,'wb')
    phrase_dir = args.phrase_dir


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
                if infile.endswith(".txt"):
                    fname = infile.split(".txt")[0]
                    phrase_dict[DIR][fname] = dict()

                    # reading the phrases in file
                    with open(phrase_dir + "/" + DIR + "/" + infile, 'rb') as foo:
                        # if "/r" in foo:
                        # foo = foo.split("/r")


                        line_num = 0
                        for line in foo:
                            if "\r" in line:
                                #print "true there is \r is here"
                                linee = line.split("\r")
                                # line = .join(line)
                                for line in linee:
                                    # print line
                                    line_num += 1

                                    # Replacing underscores with spaces
                                    line = line.decode('ascii', 'ignore').replace("_", " ").replace("\n\r", "\n")

                                    # Replacing dashes with spaces so as to prevent errors
                                    # in detecting word boundaries
                                    line = line.replace("-", " ")
                                    phrase = line.strip().split("|")[0]
                                    # print phrase

                                    # check for empty line
                                    if phrase == "":
                                        # print line_num
                                        continue

                                    if phrase not in phrase_dict:
                                        phrase_dict[DIR][fname][phrase] = 1
                                        global_dict[phrase] \
                                            = 1
                            else:
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
                                    print line_num
                                    continue

                                if phrase not in phrase_dict:
                                    phrase_dict[DIR][fname][phrase] = 1
                                    # phrase_dict[sentiments][positive][good] = 1
                                    # {sentiments:{'positive':{'good': 1, 'awesome': 1}}}
                                    # {'good': 1, 'awesome': 1, 'bad':1}

                                    global_dict[phrase] \
                                        = 1
        return phrase_dict, global_dict

# print >> outfile, global_dict

    phrase_dict, global_dict = dict_of_phrases(phrase_dir)


    for line_num, line in enumerate(infile):
        line_list = line.split("~")
        line_start = line_list[0:10]
        line_start_join = "~".join(line_start)
        aspect_tuple_1_line = line_list[10]
        aspect_tuple_1 = aspect_tuple_1_line.split(",")
        #print aspect_tuple_1 + "HEREERER"
        if aspect_tuple_1[0] == " \n":
            print "~~~~~~~~~~~~"
            #print >> outfile,
        else:
            #print "aspect"
            aspect = aspect_tuple_1[1].replace("'","").strip()
            #print aspect
            aspect_keyword = aspect_tuple_1[2].replace("'","")
            sentiment = aspect_tuple_1[4].replace("'","").replace("(","").strip()
            #print "sentiment_1 " + sentiment
            print aspect_tuple_1[5]
            sentiment_keyword = aspect_tuple_1[5].replace("u","").replace("))","").replace("'","").replace("","").replace("","").replace("NEG_","").replace("_"," ").strip()
            #sentiment_keyword = aspect_tuple_1[5]
            #print sentiment_keyword
            for category in phrase_dict:
                #print "category " + str(category)
                if category == "sentiments":
                    #print "sentiment_cat " + category
                    for sents in phrase_dict[category]:
                        #print "sentiment_files " + sents
                        if aspect == sents.split("_")[0]:
                            #print "aspect_here " +  aspect
                            #print "sentiment file " + sents.split("_")[0]
                            if sentiment_keyword in phrase_dict[category][sents]:
                                #print "aspect_found " + sents
                                #print "sent_keyword " + sentiment_keyword
                                #low price positive "not"
                                #sents.split == positive

                                if sents.split("_")[2] == 'positive':
                                    #print "to be sent " + sents.split("_")[2]
                                    if (sentiment == 'positive' or sentiment == 'most-positive') and "NEG_" not in aspect_tuple_1_line:
                                        aspect_tuple_1_line = aspect_tuple_1_line
                                    elif (sentiment == 'positive' or sentiment == 'most-positive') and "NEG_" in aspect_tuple_1_line:
                                        aspect_tuple_1_line = aspect_tuple_1_line.replace("positive","negative")
                                    elif (sentiment == 'negative' or sentiment == 'most-negative') and "NEG_" not in aspect_tuple_1_line:
                                        aspect_tuple_1_line = aspect_tuple_1_line.replace("negative","positive")
                                    elif (sentiment == 'negative' or sentiment == 'most-negative') and "NEG_" in aspect_tuple_1_line:
                                        aspect_tuple_1_line = aspect_tuple_1_line

                                elif sents.split("_")[2] == 'negative':
                                    #print "to be sent " + sents.split("_")[2]
                                    if (sentiment == 'negative' or sentiment == 'most-negative') and "NEG_" not in aspect_tuple_1_line:
                                        aspect_tuple_1_line = aspect_tuple_1_line
                                    elif (sentiment == 'negative' or sentiment == 'most-negative') and "NEG_" in aspect_tuple_1_line:
                                        aspect_tuple_1_line = aspect_tuple_1_line.replace("negative", "positive")
                                    elif (sentiment == 'positive' or sentiment == 'most-positive') and "NEG_" not in aspect_tuple_1_line:
                                        aspect_tuple_1_line = aspect_tuple_1_line.replace("positive","negative")
                                    elif (sentiment == 'positive' or sentiment == 'most-positive') and "NEG_" in aspect_tuple_1_line:
                                        aspect_tuple_1_line = aspect_tuple_1_line
                                #print "printing line"
                                #print line
            line_start_join = line_start_join + "~" + aspect_tuple_1_line
        try:
            aspect_tuple_2_line = line_list[11]
            aspect_tuple_2 = aspect_tuple_2_line.split(",")
            #print aspect_tuple_2
            #print "second aspect exists"
            #print "aspect_tuple_2 " + aspect_tuple_2
            #if (aspect_tuple_2[0] == " \n") or (aspect_tuple_2[0]== "") or (aspect_tuple_2[0] in ['\n', '\r\n']):
                #print "aspect 2 ~~~~~~~~~~~~"
                #print >> outfile,
            #else:
            if aspect_tuple_2 != "" and aspect_tuple_2 != " \n":
                #print "aspect loop here"

                #print line
                aspect_2 = aspect_tuple_2[1].replace("'","").strip()
                #print "aspect_2 " + aspect_2
                aspect_keyword_2 = aspect_tuple_2[2].replace("'","")
                #print "aspect_2_keyword " + aspect_keyword
                sentiment_2 = aspect_tuple_2[4].replace("'","").replace("(","").strip()
                sentiment_keyword_2 = aspect_tuple_2[5].replace("u'","").replace("))","").replace("'","").replace("NEG_","").replace("_"," ").strip()
                #print "sentiment_keyword_2: " + sentiment_keyword_2
                for category in phrase_dict:
                    #print "category " + str(category)
                    if category == "sentiments":
                        #print "sentiment_cat_2 " + category
                        for sents in phrase_dict[category]:
                            #print "sentiment_files_2 " + sents
                            if aspect_2 == sents.split("_")[0]:
                                #print "aspect_here_2 " +  aspect
                                #print "sentiment file " + sents.split("_")[0]
                                if sentiment_keyword_2 in phrase_dict[category][sents]:
                                    #print "aspect_found_2 " + sents
                                    #print "sent_keyword " + sentiment_keyword_2
                                    if sents.split("_")[2] == 'positive':
                                        # print "to be sent " + sents.split("_")[2]
                                        if (sentiment_2 == 'positive' or sentiment_2 == 'most-positive') and "NEG_" not in aspect_tuple_2_line:
                                            aspect_tuple_2_line = aspect_tuple_2_line
                                        elif (sentiment_2 == 'positive' or sentiment_2 == 'most-positive') and "NEG_" in aspect_tuple_2_line:
                                            aspect_tuple_2_line = aspect_tuple_2_line.replace("positive", "negative")
                                        elif (sentiment_2 == 'negative' or sentiment_2 == 'most-negative') and "NEG_" not in aspect_tuple_2_line:
                                            aspect_tuple_2_line = aspect_tuple_2_line.replace("negative", "positive")
                                        elif (sentiment_2 == 'negative' or sentiment_2 == 'most-negative') and "NEG_" in aspect_tuple_2_line:
                                            aspect_tuple_2_line = aspect_tuple_2_line

                                    elif sents.split("_")[2] == 'negative':
                                        # print "to be sent " + sents.split("_")[2]
                                        if (sentiment_2 == 'negative' or sentiment_2 == 'most-negative') and "NEG_" not in aspect_tuple_2_line:
                                            aspect_tuple_2_line = aspect_tuple_2_line
                                        elif (sentiment_2 == 'negative' or sentiment_2 == 'most-negative') and "NEG_" in aspect_tuple_2_line:
                                            aspect_tuple_2_line = aspect_tuple_2_line.replace("negative", "positive")
                                        elif (sentiment_2 == 'positive' or sentiment_2 == 'most-positive') and "NEG_" not in aspect_tuple_2_line:
                                            aspect_tuple_2_line = aspect_tuple_2_line.replace("positive", "negative")
                                        elif (sentiment_2 == 'positive' or sentiment_2 == 'most-positive') and "NEG_" in aspect_tuple_2_line:
                                            aspect_tuple_2_line = aspect_tuple_2_line
                                    #print "printing line_3"
                                    #print line
                line_start_join = line_start_join + "~" + aspect_tuple_2_line



        except:
            aspect_tuple_2_line = ""
            #print "no second aspect"
        try:
            aspect_tuple_3_line = line_list[12]
            aspect_tuple_3 = aspect_tuple_3_line.split(",")
            #print aspect_tuple_3
            #print "second aspect exists"
            #print "aspect_tuple_2 " + aspect_tuple_2
            #if (aspect_tuple_2[0] == " \n") or (aspect_tuple_2[0]== "") or (aspect_tuple_2[0] in ['\n', '\r\n']):
                #print "aspect 2 ~~~~~~~~~~~~"
                #print >> outfile,
            #else:
            if aspect_tuple_3 != "" and aspect_tuple_2 != " \n":
                #print "aspect loop here"

                #print line
                aspect_3 = aspect_tuple_3[1].replace("'","").strip()
                #print "aspect_3 " + aspect_3
                aspect_keyword_3 = aspect_tuple_3[2].replace("'","")
                #print "aspect_3_keyword " + aspect_keyword
                sentiment_3 = aspect_tuple_3[4].replace("'","").replace("(","").strip()
                #print "sentiment_3 " + sentiment_3
                sentiment_keyword_3 = aspect_tuple_3[5].replace("u'","").replace("))","").replace("'","").replace("NEG_","").replace("_"," ").strip()
                #print "sentiment_keyword_3: " + sentiment_keyword_3
                for category in phrase_dict:
                    #print "category " + str(category)
                    if category == "sentiments":
                        #print "sentiment_cat_2 " + category
                        for sents in phrase_dict[category]:
                            #print "sentiment_files_3 " + sents
                            if aspect_3 == sents.split("_")[0]:
                                #aspect_
                                #print "aspect_here_3 " +  aspect
                                #print "sentiment file " + sents.split("_")[0]
                                if sentiment_keyword_3 in phrase_dict[category][sents]:
                                    #print "aspect_found_3 " + sents
                                    #print "sent_keyword " + sentiment_keyword_3
                                    if sents.split("_")[2] == 'positive':
                                        # print "to be sent " + sents.split("_")[2]
                                        if (sentiment_3 == 'positive' or sentiment_3 == 'most-positive') and "NEG_" not in aspect_tuple_3_line:
                                            aspect_tuple_3_line = aspect_tuple_3_line
                                        elif (sentiment_3 == 'positive' or sentiment_3 == 'most-positive') and "NEG_" in aspect_tuple_3_line:
                                            aspect_tuple_3_line = aspect_tuple_3_line.replace("positive", "negative")
                                        elif (sentiment_3 == 'negative' or sentiment_3 == 'most-negative') and "NEG_" not in aspect_tuple_3_line:
                                            aspect_tuple_3_line = aspect_tuple_3_line.replace("negative", "positive")
                                        elif (sentiment_3 == 'negative' or sentiment_3 == 'most-negative') and "NEG_" in aspect_tuple_3_line:
                                            aspect_tuple_3_line = aspect_tuple_3_line

                                    elif sents.split("_")[2] == 'negative':
                                        # print "to be sent " + sents.split("_")[2]
                                        if (sentiment_3 == 'negative' or sentiment_3 == 'most-negative') and "NEG_" not in aspect_tuple_3_line:
                                            aspect_tuple_3_line = aspect_tuple_3_line
                                        elif (sentiment_3 == 'negative' or sentiment_3 == 'most-negative') and "NEG_" in aspect_tuple_3_line:
                                            aspect_tuple_3_line = aspect_tuple_3_line.replace("negative", "positive")
                                        elif (sentiment_3 == 'positive' or sentiment_3 == 'most-positive') and "NEG_" not in aspect_tuple_3_line:
                                            aspect_tuple_3_line = aspect_tuple_3_line.replace("positive", "negative")
                                        elif (sentiment_3 == 'positive' or sentiment_3 == 'most-positive') and "NEG_" in aspect_tuple_3_line:
                                            aspect_tuple_3_line = aspect_tuple_3_line

            line_start_join = line_start_join + "~" + aspect_tuple_3_line


        except:
            #aspect_tuple_3_line = ""
            print "no second aspect"
        try:
            aspect_tuple_4_line = line.split("~")[13]
            aspect_tuple_4 = aspect_tuple_4_line.split(",")
            print "second aspect exists"
            #print "aspect_tuple_2 " + aspect_tuple_2
            #if (aspect_tuple_2[0] == " \n") or (aspect_tuple_2[0]== "") or (aspect_tuple_2[0] in ['\n', '\r\n']):
                #print "aspect 2 ~~~~~~~~~~~~"
                #print >> outfile,
            #else:
            if aspect_tuple_4 != "" and aspect_tuple_4 != " \n":
                print "aspect loop here"

                aspect_4 = aspect_tuple_4[1].replace("'","").strip()
                aspect_keyword_4 = aspect_tuple_4[2].replace("'","")
                print "aspect_4_keyword " + aspect_keyword
                sentiment_4 = aspect_tuple_4[4].replace("'","").replace("(","").strip()
                sentiment_keyword_4 = aspect_tuple_4[5].replace("u'","").replace("))","").replace("'","").replace("NEG_","").replace("_"," ").strip()
                print "sentiment_keyword_4: " + sentiment_keyword_4
                for category in phrase_dict:
                    print "category " + str(category)
                    if category == "sentiments":
                        for sents in phrase_dict[category]:
                            print "sentiment_files_4 " + sents
                            if aspect_4 == sents.split("_")[0]:

                                if sentiment_keyword_4 in phrase_dict[category][sents]:

                                    if sents.split("_")[2] == 'positive':
                                        # print "to be sent " + sents.split("_")[2]
                                        if (sentiment_4 == 'positive' or sentiment_4 == 'most-positive') and "NEG_" not in aspect_tuple_4_line:
                                            aspect_tuple_4_line = aspect_tuple_4_line
                                        elif (sentiment_4 == 'positive' or sentiment_4 == 'most-positive') and "NEG_" in aspect_tuple_4_line:
                                            aspect_tuple_4_line = aspect_tuple_4_line.replace("positive", "negative")
                                        elif (sentiment_4 == 'negative' or sentiment_4 == 'most-negative') and "NEG_" not in aspect_tuple_4_line:
                                            aspect_tuple_4_line = aspect_tuple_4_line.replace("negative", "positive")
                                        elif (sentiment_4 == 'negative' or sentiment_4 == 'most-negative') and "NEG_" in aspect_tuple_4_line:
                                            aspect_tuple_4_line = aspect_tuple_4_line

                                    elif sents.split("_")[2] == 'negative':
                                        # print "to be sent " + sents.split("_")[2]
                                        if (sentiment_4 == 'negative' or sentiment_4 == 'most-negative') and "NEG_" not in aspect_tuple_4_line:
                                            aspect_tuple_4_line = aspect_tuple_4_line
                                        elif (sentiment_4 == 'negative' or sentiment_4 == 'most-negative') and "NEG_" in aspect_tuple_4_line:
                                            aspect_tuple_4_line = aspect_tuple_4_line.replace("negative", "positive")
                                        elif (sentiment_4 == 'positive' or sentiment_4 == 'most-positive') and "NEG_" not in aspect_tuple_4_line:
                                            aspect_tuple_4_line = aspect_tuple_4_line.replace("positive", "negative")
                                        elif (sentiment_4 == 'positive' or sentiment_4 == 'most-positive') and "NEG_" in aspect_tuple_4_line:
                                            aspect_tuple_4_line = aspect_tuple_4_line
                                    #print "printing line_4"
                                    #print line
            line_start_join = line_start_join + "~" + aspect_tuple_4_line



        except:
            print "no second aspect"

        try:
            aspect_tuple_5_line = line.split("~")[14]
            aspect_tuple_5 = aspect_tuple_5_line.split(",")
            print aspect_tuple_5

            #print "aspect_tuple_2 " + aspect_tuple_2
            #if (aspect_tuple_2[0] == " \n") or (aspect_tuple_2[0]== "") or (aspect_tuple_2[0] in ['\n', '\r\n']):
                #print "aspect 2 ~~~~~~~~~~~~"
                #print >> outfile,
            #else:
            if aspect_tuple_5 != "" and aspect_tuple_5 != " \n":
                print "aspect loop here"

                print line
                aspect_5 = aspect_tuple_5[1].replace("'","").strip()
                print "aspect_5 " + aspect_5
                aspect_keyword_5 = aspect_tuple_5[2].replace("'","")
                print "aspect_5_keyword " + aspect_keyword
                sentiment_5 = aspect_tuple_5[4].replace("'","").replace("(","").strip()
                sentiment_keyword_5 = aspect_tuple_5[5].replace("u'","").replace("))","").replace("'","").replace("NEG_","").replace("_"," ").strip()
                print "sentiment_keyword_5: " + sentiment_keyword_5
                for category in phrase_dict:
                    print "category " + str(category)
                    if category == "sentiments":
                        print "sentiment_cat_5 " + category
                        for sents in phrase_dict[category]:
                            print "sentiment_files_5 " + sents
                            if aspect_5 == sents.split("_")[0]:
                                print "aspect_here_5 " +  aspect
                                print "sentiment file " + sents.split("_")[0]
                                if sentiment_keyword_5 in phrase_dict[category][sents]:
                                    print "aspect_found_5 " + sents
                                    print "sent_keyword " + sentiment_keyword_5
                                    if sents.split("_")[2] == 'positive':
                                        # print "to be sent " + sents.split("_")[2]
                                        if (sentiment_5 == 'positive' or sentiment_5 == 'most-positive') and "NEG_" not in aspect_tuple_5_line:
                                            aspect_tuple_5_line = aspect_tuple_5_line
                                        elif (sentiment_5 == 'positive' or sentiment_5 == 'most-positive') and "NEG_" in aspect_tuple_5_line:
                                            aspect_tuple_5_line = aspect_tuple_5_line.replace("positive", "negative")
                                        elif (sentiment_5 == 'negative' or sentiment_5 == 'most-negative') and "NEG_" not in aspect_tuple_5_line:
                                            aspect_tuple_5_line = aspect_tuple_5_line.replace("negative", "positive")
                                        elif (sentiment_5 == 'negative' or sentiment_5 == 'most-negative') and "NEG_" in aspect_tuple_5_line:
                                            aspect_tuple_5_line = aspect_tuple_5_line

                                    elif sents.split("_")[2] == 'negative':
                                        # print "to be sent " + sents.split("_")[2]
                                        if (sentiment_5 == 'negative' or sentiment_5 == 'most-negative') and "NEG_" not in aspect_tuple_5_line:
                                            aspect_tuple_5_line = aspect_tuple_5_line
                                        elif (sentiment_5 == 'negative' or sentiment_5 == 'most-negative') and "NEG_" in aspect_tuple_5_line:
                                            aspect_tuple_5_line = aspect_tuple_5_line.replace("negative", "positive")
                                        elif (sentiment_5 == 'positive' or sentiment_5 == 'most-positive') and "NEG_" not in aspect_tuple_5_line:
                                            aspect_tuple_5_line = aspect_tuple_5_line.replace("positive", "negative")
                                        elif (sentiment_5 == 'positive' or sentiment_5 == 'most-positive') and "NEG_" in aspect_tuple_5_line:
                                            aspect_tuple_5_line = aspect_tuple_5_line

                                    print "printing line_5"
                                    print line
            line_start_join = line_start_join + "~" + aspect_tuple_5_line
        except:
            print "no second aspect"
            print "skipped aspect tuple 5"
        print "final line " + line_start_join
        print >> outfile, line_start_join + " ~ "

    infile.close()
    outfile.close()







