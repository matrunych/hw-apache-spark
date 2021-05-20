from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import countDistinct, collect_list, array, element_at, sum
import json


def top10_trends(data):
    result = {"videos": []}
    videos = data.groupBy(["video_id", "title", "description"]) \
        .agg(collect_list(array('trending_date', "views", "likes", "dislikes")) \
             .alias("trending_days"), countDistinct("trending_date"))

    videos = videos.orderBy("count(trending_date)", ascending=False)
    videos = videos.withColumn("latest_views", element_at("trending_days", -1)[1]) \
        .withColumn("latest_likes", element_at("trending_days", -1)[2]) \
        .withColumn("latest_dislikes", element_at("trending_days", -1)[3])

    for row in videos.rdd.collect():
        result["videos"].append({"id": row["video_id"], "title": row["title"], "description": row["description"],
                                 "latest_views": row["latest_views"], "trending_days": row["trending_days"]})

    result["videos"] = result["videos"][:10]
    return result


def top20_channels(data):
    result = {"channels": []}
    channels = data.groupBy(["channel_title"]) \
        .agg(collect_list(array('video_id', 'trending_date', "views")).alias("trending_days"),
             countDistinct("video_id"),
             sum("views").alias("all_views")).orderBy("all_views", ascending=False).head(20)

    i = 0
    for row in channels:
        result["channels"].append({"channel_name": row["channel_title"], "start_date": row["trending_days"][0][1],
                                   "end_date": row["trending_days"][-1][1], "total_view": row["all_views"],
                                   "videos_views": []})
        for video in row["trending_days"]:
            result["channels"][i]["videos_views"].append({"video_id": video[0], "views": video[2]})
        i += 1

    return result


def top10_channels_by_trends(data):
    result = {"channels": []}

    videos = data.groupBy(["channel_title", "video_id", "title", "description"]) \
        .agg(collect_list(array('trending_date', "views")) \
             .alias("trending_days"), countDistinct("trending_date"))

    channels = data.groupBy(["channel_title"]) \
        .agg(collect_list(array('video_id', "title", 'trending_date', "views")).alias("trending_days"),
             countDistinct("trending_date")
             ).orderBy("count(trending_date)", ascending=False).head(10)

    i = 0
    for row in channels:
        all_videos = videos.where(videos["channel_title"] == row["channel_title"])

        result["channels"].append(
            {"channel_name": row["channel_title"], "total_trending_days": row["count(trending_date)"],
             "videos_days": []})
        for video in all_videos.collect():
            result["channels"][i]["videos_days"].append({"video_id": video["video_id"], "video_title": video["title"],
                                                         "trending_days": video["count(trending_date)"]})
        i += 1

    return result


if __name__ == "__main__":
    sc = SparkContext('local[*]')
    spark = SparkSession.builder.appName("to read csv file").getOrCreate()
    df = spark.read.option("header", "true").csv('dt/USvideos.csv')

    res = top10_trends(df)

    with open("result.json", "w") as outfile:
        json.dump(res, outfile, indent=4)
