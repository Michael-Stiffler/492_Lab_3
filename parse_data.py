from pymongo import MongoClient
import pymongo
import json
import spacy


def main():
    connection = MongoClient('localhost', 27017)
    db = connection.LAB3
    collection = db.tweets
    cursor = collection.find({})

    stop_words = load_stop_words()

    parse_data(cursor, stop_words)


def load_stop_words():
    stop_words = []

    with open("stop_word_list.txt") as f:
        lines = f.readlines()

        for line in lines:
            stop_words.append(line.strip('\n'))

        f.close()

    return stop_words


def parse_data(cursor, stop_words):
    counter = 0
    sp = spacy.load('en_core_web_sm')

    word_dict = {}

    for document in cursor:

        try:
            full_text = document["retweeted_status"]["extended_tweet"]["full_text"]
        except:
            pass
        try:
            full_text = document["extended_tweet"]["full_text"]
        except:
            pass
        try:
            full_text = document["text"]
        except:
            pass

        sentence = sp(full_text)

        for word in sentence:

            lemma = word.lemma_

            lemma = ''.join(i for i in str(lemma) if i.isalnum())
            lemma = lemma.lower()
            # and word.pos_ != "PUNCT" and word.pos_ != "SYM" and word.pos_ != "NUM"
            if len(lemma) > 0 and lemma not in stop_words and not find_num(lemma):
                if lemma in word_dict:
                    word_dict[lemma] += 1
                else:
                    word_dict[lemma] = 1
        counter += 1
        print(counter)

    counter = 0
    for item in sorted(word_dict, key=word_dict.get, reverse=True):
        if counter > 15:
            break
        else:
            with open("top_15_words.txt", "a") as f:
                f.write(str(item) + " , " + str(word_dict[item]) + "\n")
                counter += 1


def find_num(word):
    return any(i.isdigit() for i in word)


if __name__ == "__main__":
    main()
