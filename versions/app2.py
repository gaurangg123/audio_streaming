from flask import Flask, render_template
from flask_socketio import SocketIO
import requests
import base64
import threading
import csv
import re

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret_key'
socketio = SocketIO(app, cors_allowed_origins="*")

audio_urls = []

URL_PATTERN = re.compile(r'^(https?://)')

# Load audio URLs from CSV
def load_audio_urls():
    global audio_urls
    audio_urls = []
    with open('audio_urls.csv', 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            if row and row[0]:
                url = row[0].strip()
                if URL_PATTERN.match(url):
                    audio_urls.append(url)
                else:
                    print(f"Ignored invalid URL: {url}")
    print(f"Loaded {len(audio_urls)} audio URLs.")

@app.route('/')
def index():
    return render_template('index.html')

# Stream audio chunks
def stream_audio(audio_url, session_id):
    try:
        response = requests.get(audio_url, stream=True, timeout=10)
        print(f"Starting audio stream for session {session_id}...")
        chunk_count = 0
        flags = []
        for chunk in response.iter_content(chunk_size=1024):
            encoded_chunk = base64.b64encode(chunk).decode('utf-8')
            chunk_count += 1
            flag = None
            if chunk_count % 10 == 0:
                flag = f"Flag {chunk_count // 10}"
                flags.append(flag)
                print(f"Session {session_id}: {flag}")

            socketio.emit(
                'audio_chunk',
                {'data': encoded_chunk, 'session_id': session_id, 'flag': flag},
                namespace='/audio_stream'
            )
        print(f"Audio streaming complete for session {session_id}.")
        socketio.emit(
            'stream_complete',
            {'session_id': session_id, 'flags': flags},
            namespace='/audio_stream'
        )
    except Exception as e:
        print(f"Error streaming session {session_id}: {e}")

# Start streaming for all valid audio URLs
def start_streams():
    threads = []
    for i, url in enumerate(audio_urls):
        thread = threading.Thread(target=stream_audio, args=(url, i))
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()

@socketio.on('start_stream', namespace='/audio_stream')
def handle_start_stream(data):
    load_audio_urls()
    print(f"Streaming {len(audio_urls)} audio files...")
    start_streams()

if __name__ == '__main__':
    socketio.run(app, debug=True)
