from flask import Flask
from flask_sock import Sock
import asyncio
import json
import base64
import uuid
import ffmpeg
import threading
import time

app = Flask(__name__)
sock = Sock(app)

STREAM_URLS = [
    "https://s3.us-east-1.amazonaws.com/twilio-calls-recordings/recordings/ACab32b204986a87a022a07b5cf4c95e0f/REaf0c60c181e2f36b0be8a20c80056767",
    "https://mediaserv33.live-streams.nl:8034/live",
]


async def send_flags(ws, stream_uuid, flag_type, sequence_number):
    flag = {
        "event": flag_type.lower(),
        "sequenceNumber": str(sequence_number),
        "flag_type": flag_type,
        "streamSid": stream_uuid,
    }
    if flag_type == "Start Flag":
        flag["start"] = {"streamSid": stream_uuid}
    elif flag_type == "Stop Flag":
        flag["stop"] = {"streamSid": stream_uuid}

    await ws.send(json.dumps(flag))


def process_audio(ws, stream_url, stream_uuid, loop):
    async def send_stop_flag():
        await send_flags(ws, stream_uuid, "Stop Flag", 999)

    def run_ffmpeg():
        try:
            process = (
                ffmpeg.input(stream_url)
                .output("pipe:", format="wav", acodec="pcm_s16le", ac=1, ar=8000)
                .run_async(pipe_stdout=True, pipe_stderr=True)
            )

            chunk_counter = 0
            while True:
                chunk = process.stdout.read(4096)
                if not chunk:
                    break

                encoded_chunk = base64.b64encode(chunk).decode("utf-8")
                message = {
                    "event": "media",
                    "sequenceNumber": str(chunk_counter + 2),
                    "media": {
                        "track": "inbound",
                        "chunk": str(chunk_counter + 1),
                        "timestamp": str(chunk_counter * 30),
                        "payload": encoded_chunk,
                    },
                    "streamSid": stream_uuid,
                }

                future = asyncio.run_coroutine_threadsafe(
                    ws.send(json.dumps(message)), loop
                )
                try:
                    future.result()
                except Exception as e:
                    print(f"[ERROR] WebSocket send error: {e}")
                    break

                chunk_counter += 1

            process.wait()
            asyncio.run_coroutine_threadsafe(send_stop_flag(), loop)

        except Exception as e:
            print(f"[ERROR] FFmpeg error: {e}")

    thread = threading.Thread(target=run_ffmpeg, daemon=True)
    thread.start()


@sock.route("/stream")
def stream_handler(ws):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    for stream_url in STREAM_URLS:
        stream_uuid = str(uuid.uuid4())
        loop.run_until_complete(send_flags(ws, stream_uuid, "Start Flag", 1))
        threading.Thread(
            target=process_audio, args=(ws, stream_url, stream_uuid, loop), daemon=True
        ).start()

    try:
        while True:
            time.sleep(1)
    except Exception:
        pass


@app.route("/")
def home():
    return "✅ WebSocket server running. Connect to `/stream` via WS."


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
