from flask import Flask, render_template
from flask_socketio import SocketIO
import requests
import base64
import threading

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret_key'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="eventlet")

audio_urls = [
    "https://s3.us-east-1.amazonaws.com/twilio-calls-recordings/recordings/ACab32b204986a87a022a07b5cf4c95e0f/REef751a16d3e8c36961a19ee8c9a1d5fb",
    "https://s3.us-east-1.amazonaws.com/twilio-calls-recordings/recordings/ACab32b204986a87a022a07b5cf4c95e0f/REfc56fc6b4364f4f5caeddb7f582c7546",
    "https://s3.us-east-1.amazonaws.com/twilio-calls-recordings/recordings/ACab32b204986a87a022a07b5cf4c95e0f/REba4047f0228b9e7f463b71b18fc18ef7",
    "https://s3.us-east-1.amazonaws.com/twilio-calls-recordings/recordings/ACab32b204986a87a022a07b5cf4c95e0f/REbc75027f1b115d54dc275454b2c22661"
]

@app.route('/')
def index():
    return render_template('index.html')

def stream_audio(audio_url, session_id):
    response = requests.get(audio_url, stream=True)
    print(f"Starting audio stream for session {session_id}...")
    chunk_count = 0
    for chunk in response.iter_content(chunk_size=1024):
        encoded_chunk = base64.b64encode(chunk).decode('utf-8')
        chunk_count += 1
        flag = None
        if chunk_count % 10 == 0:  # Emit a flag every 10 chunks
            flag = f"Flag {chunk_count // 10}"
            print(f"Session {session_id}: Emitting {flag}")
        socketio.emit('audio_chunk', {'data': encoded_chunk, 'session_id': session_id, 'flag': flag}, namespace='/audio_stream')
        socketio.sleep(0)  # Ensure immediate emission of chunks
    print(f"Audio streaming complete for session {session_id}.")

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
    print("Client requested to start streams...")
    start_streams()

@socketio.on('flag_received', namespace='/audio_stream')
def handle_flag_received(data):
    print(f"Confirmation received for {data['flag']} from Audio {data['session_id'] + 1}")

if __name__ == '__main__':
    socketio.run(app, debug=True, port=5000)
