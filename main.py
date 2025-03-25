import pickle, os, asyncio,requests, traceback
from motor.motor_asyncio import AsyncIOMotorClient
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()
credentials = None

async def main():
    start = datetime.now()
    authenticaiton()
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, credentials=credentials)
    videoCount = []
    settings = await get_settings()
    categories = settings["webhooks"].keys()
    categories -= {"status", "doctrzombie"}
    last_sync = datetime.strptime(settings["last_sync"], "%Y-%m-%d %H:%M:%S")
    delta_uptime = datetime.now() - last_sync
    total_sec = delta_uptime.total_seconds()
    minutes, seconds = divmod(int(total_sec), 60)
    print(f"Time {minutes}:{seconds}sec")
    time = datetime.now() - timedelta(seconds=total_sec)
    iso_time = time.replace(tzinfo=timezone.utc).isoformat()
    for category in list(categories):
        channels = await get_channel_ids(category)
        url = settings['webhooks'][category]
        if not channels:
            print(f"No channels found for {category}")
            continue
        data = get_videos(youtube, channels, iso_time, url)
        videoCount.append(f"{category} = {len(data)}\n")
        await add_youtube_data(data=data)

    data = {'last_sync': datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    await update_settings("set", data)
    end = datetime.now()
    execution = end - start
    message = f'Here are the stats boss \n```json\n{"".join(videoCount)}```This took `{execution}s` to complete. '
    response = requests.post(settings["webhooks"]['status'], json={"content":message})
    print(message + str(response.status_code))

    return

def authenticaiton():
    global credentials
    client_secret = os.path.normpath(os.getcwd() + os.sep + os.pardir)
    if os.path.exists("token.pickle"):
        print("Loading Credentials From File...")
        with open("token.pickle", "rb") as token:
            credentials = pickle.load(token)

    # If there are no valid credentials available, then either refresh the token or log in.
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            print("Refreshing Access Token...")
            credentials.refresh(Request())
            return
        print("Fetching New Tokens...")
        flow = InstalledAppFlow.from_client_secrets_file(
            f"{client_secret}/json/client_secret.json",
            scopes=["https://www.googleapis.com/auth/youtube.readonly"],
        )

        flow.run_local_server(
            port=8080, prompt="consent", authorization_prompt_message=""
        )
        credentials = flow.credentials

        # Save the credentials for the next run
        with open("token.pickle", "wb") as f:
            print("Saving Credentials for Future Use...")
            pickle.dump(credentials, f)
        return


async def get_settings():
    """Returns a list of channel objects with a given category"""

    client = AsyncIOMotorClient(os.getenv("KnightBot_database"))
    youtube = client.youtube
    settings = youtube.settings
    settings = await settings.find_one({})
    return settings


async def get_channel_ids(category):
    """Returns a list of channel objects with a given category"""

    client = AsyncIOMotorClient(os.getenv("KnightBot_database"))
    youtube = client.youtube
    youtube_channels = youtube.youtube_channels
    pipeline = [
        {
            "$match": {"category": category}
        },  # Match the documents where 'category' equals the given value
        {
            "$project": {"_id": 0, "channel_id": 1}
        },  # Project only the 'channel_id', exclude '_id'
    ]
    youtube_channel_ids = youtube_channels.aggregate(pipeline)
    data = []
    async for channel in youtube_channel_ids:
        data.append(channel["channel_id"])
    if data:
        return data
    return False


def get_videos(youtube, channels, iso_time, url):
    video_data = []
    for channel in channels:
        video = youtube.activities().list(
            part="snippet,contentDetails",
            channelId=channel,
            publishedAfter=iso_time,
            maxResults=50,
        )
        resp_vdo = video.execute()
        for i in resp_vdo["items"]:
            try:
                video_title = i["snippet"]["title"]
                video_pub_time = i["snippet"]["publishedAt"]
                if video_title:
                    video_link = i["contentDetails"]["upload"]["videoId"]
                    usable_data = {
                        "publishedAt": i["snippet"]["publishedAt"],
                        "channelId": i["snippet"]["channelId"],
                        "title": i["snippet"]["title"],
                        "thumbnails": i["snippet"]["thumbnails"]["high"],
                        "url": f"https://youtu.be/{video_link}",
                        "type": i["snippet"]["type"],
                    }
                    video_data.append(usable_data)
                    response = requests.post(url, json={"content":f"https://youtu.be/{video_link}"})
                    print(video_title + "\t" + video_pub_time + "\t" + str(response.status_code))
            except KeyError:
                pass
    return video_data

async def add_youtube_data(data):
    client = AsyncIOMotorClient(os.getenv('KnightBot_database'))
    youtube = client.youtube
    youtube_data = youtube.youtube_data
    for i in data:
        youtube_data.insert_one(i)
    return True

async def update_settings(action, data):
    "data is an object that will be added/removed depending on type"
    client = AsyncIOMotorClient(os.getenv('KnightBot_database'))
    youtube = client.youtube
    settings = youtube.settings
    try:
        if action == "set":
            stuff_to_add = { "$set": data}
        else:
            stuff_to_add = { "$unset": data}

        settings.update_one({}, stuff_to_add)
        return True
    except:
        print(traceback.format_exc())
        return False
asyncio.run(main())