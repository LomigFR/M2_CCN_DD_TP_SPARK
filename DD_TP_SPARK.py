#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Template for the job scripts of Spark tutorials."""
import os
import sys

# Define parameters
SPARK_HOME = '/home/guillaume_collet/Documents/DD/TP3_Spark_PUBG/spark-2.4.4-bin-hadoop2.7'
PY4J_ZIP = 'py4j-0.10.7-src.zip'
SPARK_MASTER = 'local[*]'  # Or "spark://[MASTER_IP]:[MASTER_PORT]"
os.environ['PYSPARK_PYTHON'] = 'python3.7'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python3.7'

# Add pyspark to the path and import this module
sys.path.append(SPARK_HOME + '/python')
sys.path.append(SPARK_HOME + '/python/lib/' + PY4J_ZIP)
from pyspark import SparkConf, SparkContext, StorageLevel  # noqa


# Create a Spark configuration object, needed to create a Spark context
# The master used here is a local one, using every available CPUs
spark_conf = SparkConf().setMaster(SPARK_MASTER)
sc = SparkContext(conf=spark_conf)

###################################################################################


def print_top_words_index(item_set):
    """Print the top words index, input format: ((occurences, word), index)."""
    for item in item_set:
        occurences_word, index = item  # Unpack ((occurences, word), index)
        occurences, word = occurences_word  # Unpack (occurences, word)
        print('%d: %s (%d)' % (index, word, occurences))


def moyenne(liste):
    return (sum(liste)/len(liste), len(liste))

###################################################################################

# Traitement préalable via le terminal :
# Copier les 100000 premières lignes dans un autre fichier : head -n 100000 "agg_match_stats_0.csv" > dd_tp3_match_stat.csv


# Define here your processes
try:
    # Load the RDD as a text file and persist it (cached into memory or disk)
    file_name = "../dd_tp3_match_stat.csv"
    game = sc.textFile(os.path.join(SPARK_HOME, file_name))
    first_line = game.first()
    game = game.filter(lambda line: line != first_line)
    game.persist()

    # 3. Pour chaque joueur, ne conserver que son nom et son player_kills :
    player_kills = game.map(lambda row: row.split(
        ",")).map(lambda item: (item[11], int(item[10])))
    # print(player_kills.take(5))

    # 4. Pour chaque joueur, afficher la somme de ses player_kills :
    sum_player_kills = player_kills.reduceByKey(lambda a, b: a + b)
    # print(sum_player_kills.take(50))

    # 5. Pour chaque joueur, afficher le nombre de parties jouées :
    number_player_matches = player_kills.groupByKey().mapValues(lambda iter: len(iter))
    # print(number_player_matches.take(50))

    # 5bis. Pour chaque jouer, afficher le nombre moyen de kills par partie :
    player_mean_kills = player_kills\
        .groupByKey()\
        .mapValues(moyenne)
    # print(player_mean_kills.take(100))

    # 6. Obtenir les 10 meilleurs joueurs selon leur player_kills :
    best_players = sum_player_kills.sortBy(
        lambda paire: paire[1], ascending=False)
    # print(best_players.take(50))

    # 7. Ne conserver que les joueurs ayant joué au moins 4 parties :
    best_players_4 = player_mean_kills.filter(lambda item: item[1][1] >= 4)\
        .sortBy(lambda paire: paire[1][0], ascending=False)
    # print(best_players_4.take(50))

    # Traitement joueur particulier :
    without_strange_gamer = best_players_4.filter(lambda item: item[0] != '')
    print(without_strange_gamer.take(10))
    # Count the number of items (= lines)
    # print('\Il y a %d lignes dans le fichier' %
    #      game.count())

    # Print the first line
    # print('\nLa 1ère ligne de ce fichier contient : "%s"' % game.first())

    # Get the words and their occurences (the two methods are equivalent)
    # word_occs_rdd = game.flatMap(lambda line: line.split(' '))\
    #    .map(lambda line: (line, 1))\
    #    .reduceByKey(lambda a, b: a+b)
    # print('\nWord occurences : %s' % word_occs_rdd.take(10))

    # Another method which is shorter
    # word_occurences = readme_rdd.flatMap(lambda line: line.split(' '))\
    #                             .countByValue()

    # Sorted word occurences
    # sorted_word_occs_rdd = word_occs_rdd.map(lambda item: (item[1], item[0]))\
    #                                    .sortByKey(ascending=False)
    # print('\nTop 50 words : %s' % sorted_word_occs_rdd.take(50))

    # Add the index to the top 50 words, format is
    # top_words_w_index_rdd = sorted_word_occs_rdd.zipWithIndex()
    # print('\nTop 50 words with index:')
    # print_top_words_index(top_words_w_index_rdd.take(50))

    # Remove the empty character
    # top_words_rdd = top_words_w_index_rdd.filter(lambda item: item[0][1] != '')
    # print('\nTop 50 words with index (empty character filtered out):')
    # print_top_words_index(top_words_rdd.take(50))

    # Unpersist the readme RDD
    game.unpersist()

# Except an exception, the only thing that it will do is to throw it again
except Exception as e:
    raise e

# NEVER forget to close your current context
finally:
    sc.stop()
