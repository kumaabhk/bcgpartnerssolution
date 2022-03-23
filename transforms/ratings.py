from pyspark.sql.functions import flatten
from pyspark.sql.functions import explode
class Ratings():
    def __init__(self,spark):
        self.spark = spark

    def transform(self):

        # read the data files as taken from download
        # in case of huge volume, I would expect the data to be saved in HDFS location if infra is On- Premises then code will be changed
        # to read data from HDFS

        df_ratings = self.spark.read.csv('../data/title.ratings.tsv', sep=r'\t', header=True).select('tconst', 'averageRating', 'numVotes')
        print(df_ratings.printSchema())

        df_name = self.spark.read.csv('../data/name.basics.tsv', sep=r'\t', header=True).select('nconst', 'primaryName',
                                                                                                'birthYear', 'deathYear', 'primaryProfession', 'knownForTitles')
        df_title_basics = self.spark.read.csv('../data/title.basics.tsv', sep=r'\t', header=True).select('tconst',
                                                                                                'primaryTitle'
                                                                                            )
        df_name_basics = self.spark.read.csv('../data/name.basics.tsv', sep=r'\t', header=True).select('nconst',
                                                                                                         'primaryName',
                                                                                                         'knownForTitles'
                                                                                                         )
        averageNoOfVotes = df_ratings.agg({'numVotes': 'mean'}).alias("avgNoVote") # calculate average no of votes
        vall = averageNoOfVotes.head(1)[0][0] # since its a single value, saved in variable

        df_ratings = df_ratings.filter(df_ratings["numVotes"] > 50) # reduce data volume upfront by filtering numVotes > 50
        df_ratings = df_ratings.withColumn("rank", (df_ratings['numVotes']/vall)*df_ratings['averageRating']) # find out Rank as given in the question

        df_ratings = df_ratings.sort(df_ratings.rank, ascending=False) # preferred sorting as it will work efficiently on large dataset as compared to orderby
        df_sample_list = df_ratings.head(20) # as per question, listed out top 20 rows
        cols = ["tconst_ratings","averageRating","numVotes","rank"]
        df_sample = self.spark.createDataFrame(data=df_sample_list, schema=cols) # created dataframe for top 20 rows
        print(df_sample.head())

        # joined with title_basics to get the primary name of the movie
        # This results the top 20 movies for numvotes > 50 based on ranking along with the primary name
        #df_movie = df_sample.join(df_title_basics, df_sample.tconst_ratings == df_title_basics.tconst, how="left")

        # Solution for 2nd part of question
        print(df_name_basics.printSchema())

        df_name_basics = df_name_basics.select("nconst","primaryName", explode(df_name_basics.knownForTitles))

        return df_ratings, df_name




