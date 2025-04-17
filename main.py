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
    client_secret = os.getenv("client_secret_path")
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
            f"{client_secret}",
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


from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import isodate
import requests

def get_videos(youtube, channels, iso_time, url):
    video_data = []
    for channel in channels:
        try:
            # Fetch the list of activities for the channel
            activities = youtube.activities().list(
                part="contentDetails",
                channelId=channel,
                publishedAfter=iso_time,
                maxResults=50,
            ).execute()

            # Extract video IDs from the activities
            video_ids = [
                item["contentDetails"]["upload"]["videoId"]
                for item in activities.get("items", [])
                if "upload" in item["contentDetails"]
            ]

            if not video_ids:
                continue

            # Fetch video details including duration
            video_response = youtube.videos().list(
                part="snippet,contentDetails",
                id=",".join(video_ids),
            ).execute()

            for video in video_response.get("items", []):
                duration = isodate.parse_duration(video["contentDetails"]["duration"])
                minimum_duration = int(os.getenv("minimum_duration"))
                if duration.total_seconds() > minimum_duration:  # Exclude videos 60 seconds or shorter
                    video_title = video["snippet"]["title"]
                    video_pub_time = video["snippet"]["publishedAt"]
                    video_link = f"https://youtu.be/{video['id']}"
                    usable_data = {
                        "publishedAt": video_pub_time,
                        "channelId": video["snippet"]["channelId"],
                        "title": video_title,
                        "thumbnails": video["snippet"]["thumbnails"]["high"],
                        "url": video_link,
                        "type": video["kind"],
                    }
                    video_data.append(usable_data)
                    response = requests.post(url, json={"content": video_link})
                    print(f"{video_title}\t{video_pub_time}\t{response.status_code}")
                else:
                    print(f"Ignoring short {video["snippet"]["title"]}")
        except HttpError as e:
            print(f"An HTTP error {e.resp.status} occurred: {e.content}")
        except KeyError as e:
            print(f"KeyError: {e}")
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