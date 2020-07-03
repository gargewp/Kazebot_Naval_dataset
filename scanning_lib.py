import requests
import json
import time
import numpy as np
import pandas as pd
import os


# Get comments from certain video
def scan_single_video(video_id, author_video_name, api_key):
    text_format = "plainText"

    column_names_comment = ["id",
                            "videoId",
                            "authorVideoName",
                            "totalReplyCount",
                            "likeCount",
                            "authorDisplayName",
                            "textDisplay",
                            "publishedAt", "isReply"]

    # Запись коммента верхнего уровня (CommentThread)
    def make_record_comment(_item):
        return [_item["id"],
                _item["snippet"]["videoId"],
                author_video_name,
                _item["snippet"]["totalReplyCount"],
                _item["snippet"]["topLevelComment"]["snippet"]["likeCount"],
                _item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                _item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                _item["snippet"]["topLevelComment"]["snippet"]["publishedAt"],
                0]

    # Запись коммента второго уровня (Comment)
    def make_record_reply(_item_message, _video_id, _author_video_name):
        return [_item_message["id"],
                _video_id,
                _author_video_name,
                0,
                _item_message["snippet"]["likeCount"],
                _item_message["snippet"]["authorDisplayName"],
                _item_message["snippet"]["textDisplay"],
                _item_message["snippet"]["publishedAt"],
                1]

    # Формируем строку запроса

    n_results = 100

    url_string = """https://www.googleapis.com/youtube/v3/commentThreads?""" \
                 + """key=""" + api_key \
                 + """&textFormat=""" + text_format \
                 + """&part=snippet,replies""" \
                 + """&videoId=""" + video_id \
                 + """&maxResults=""" + str(n_results) \
                 + """&fields=nextPageToken,""" \
                 + """items(id,snippet(videoId,totalReplyCount,isPublic,""" \
                 + """topLevelComment(snippet(textDisplay,likeCount,authorDisplayName,publishedAt))),""" \
                 + """replies(comments(id,snippet(videoId,textDisplay,authorDisplayName,likeCount,publishedAt))))"""

    # Собираем треды верхнего уровня и подкомменты если их не больше 5 штук
    # Всё что больше 5 штук уходит для последующего извлечения

    record_list = []
    reply_containers = []

    r = requests.get(url_string)
    json_data = r.json()
    next_page_token = json_data.get("nextPageToken")

    for item in json_data["items"]:
        record_list.append(make_record_comment(item))
        if item["snippet"]["totalReplyCount"] > 5:
            reply_containers.append(
                [item["id"], item["snippet"]["videoId"], author_video_name, item["snippet"]["totalReplyCount"]])
        elif item["snippet"]["totalReplyCount"] > 0:
            for item_message in item["replies"]["comments"]:
                record_list.append(make_record_reply(item_message, item["snippet"]["videoId"], author_video_name))

    print('Comments and replies collected: ' + str(len(record_list)))

    time.sleep(0.1)

    while next_page_token:
        r = requests.get(url_string + "&pageToken=" + next_page_token)
        json_data = r.json()
        next_page_token = json_data.get("nextPageToken")

        for item in json_data["items"]:
            record_list.append(make_record_comment(item))
            if item["snippet"]["totalReplyCount"] > 5:
                reply_containers.append(
                    [item["id"], item["snippet"]["videoId"], author_video_name, item["snippet"]["totalReplyCount"]])
            elif item["snippet"]["totalReplyCount"] > 0:
                for item_message in item["replies"]["comments"]:
                    record_list.append(make_record_reply(item_message, item["snippet"]["videoId"], author_video_name))

        print('Comments and replies collected: ' + str(len(record_list)))
        if len(json_data["items"]) < n_results:
            break
        time.sleep(0.1)

    # Извлекаем большие треды
    for reply_container in reply_containers:

        parent_id = reply_container[0]
        n_results = 100

        url_string = """https://www.googleapis.com/youtube/v3/comments?""" \
                     + """key=""" + api_key \
                     + """&textFormat=""" + text_format \
                     + """&part=snippet""" \
                     + """&parentId=""" + parent_id \
                     + """&maxResults=""" + str(n_results) \
                     + """&fields=nextPageToken,items(id,snippet(textDisplay,authorDisplayName,likeCount,publishedAt))"""

        r = requests.get(url_string)
        json_data = r.json()
        next_page_token = json_data.get("nextPageToken")

        record_list.extend([make_record_reply(item_message, reply_container[1], reply_container[2]) for item_message in
                            json_data["items"]])

        print('Comments and replies collected: ' + str(len(record_list)))

        time.sleep(0.1)

        while next_page_token:
            r = requests.get(url_string + "&pageToken=" + next_page_token)
            json_data = r.json()
            next_page_token = json_data.get("nextPageToken")

            record_list.extend(
                [make_record_reply(item_message, reply_container[1], reply_container[2]) for item_message in
                 json_data["items"]])

            print('Comments and replies collected: ' + str(len(record_list)))

            if len(json_data["items"]) < n_results:
                break

            time.sleep(0.1)
    return record_list


# Get info about videos on certain channel
def scan_single_channel(channel_id, time_lower_bound, api_key):
    n_results = 50
    url_string = """https://www.googleapis.com/youtube/v3/search?""" \
                 + """key=""" + api_key \
                 + """&channelId=""" + channel_id \
                 + """&part=snippet&order=date&fields=nextPageToken,items(id)""" \
                 + """&maxResults=""" + str(n_results) \
                 + """&publishedAfter=""" + time_lower_bound

    video_id_list = []

    r = requests.get(url_string)
    json_data = r.json()
    next_page_token = json_data.get("nextPageToken")

    current_page_video_id_list = [item["id"]["videoId"] for item in json_data["items"] if
                                  item["id"]["kind"] == 'youtube#video']

    video_id_list.extend(current_page_video_id_list)
    print('Videos collected ' + str(len(video_id_list)))
    time.sleep(0.1)

    while next_page_token:
        r = requests.get(url_string + "&pageToken=" + next_page_token)
        json_data = r.json()
        next_page_token = json_data.get("nextPageToken")

        current_page_video_id_list = [item["id"]["videoId"] for item in json_data["items"] if
                                      item["id"]["kind"] == 'youtube#video']
        video_id_list.extend(current_page_video_id_list)
        print('Videos collected ' + str(len(video_id_list)))
        if len(json_data["items"]) < n_results:
            break
        time.sleep(0.1)

    # n_results = 50
    # value = len(video_id_list)

    # Индексы поиска
    search_idxs = [[i * 50, min(len(video_id_list), (i + 1) * 50)] for i in
                   range(int(np.ceil(len(video_id_list) / n_results)))]
    # Разбивка по 50 видео через запятую
    video_id_split_list = [",".join(video_id_list[idxes[0]:idxes[1]]) for idxes in search_idxs]

    # Названия столбцов
    column_names = ['id',
                    'title',
                    'publishedAt',
                    'channelTitle',
                    'viewCount',
                    'likeCount',
                    'dislikeCount',
                    'commentCount']

    def safe_record(val):
        try:
            return val
        except:
            return 0

    # Из json в строку для записи в таблицу
    def make_record(item):
        return [item['id'],
                item['snippet']['title'],
                item['snippet']['publishedAt'],
                item['snippet']['channelTitle'],
                item['statistics']['viewCount'],
                item['statistics']['likeCount'],
                item['statistics']['dislikeCount'],
                item['statistics']['commentCount']]

    record_list = []
    # Обрабатываем респонс по каждому списку
    for videoId in video_id_split_list:
        # Формируем строку запроса
        api_key = "AIzaSyCPscCvM_mHmn0EGjW5wOj-sJcUf5x0JOE"

        url_string_video = """https://www.googleapis.com/youtube/v3/videos?""" \
                           + """id=""" + videoId \
                           + """&key=""" + api_key \
                           + """&part=snippet,statistics""" \
                           + """&fields=nextPageToken,""" \
                           + """items(id, snippet(title, channelTitle, publishedAt),statistics(viewCount,likeCount,dislikeCount,commentCount))"""

        r = requests.get(url_string_video)
        json_data = r.json()

        # record_list.extend([make_record(item) for item in json_data["items"]])
        for item in json_data["items"]:
            try:
                record_list.append(make_record(item))
            except:
                0

        print("Videos recorded: " + str(len(record_list)))
        time.sleep(0.1)

    return record_list
