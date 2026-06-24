# Copyright 2023 LiveKit, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from dataclasses import dataclass
from typing import TYPE_CHECKING, AsyncIterator, List, Optional, Union

from ._ffi_client import FfiHandle, FfiClient
from ._proto import ffi_pb2 as proto_ffi
from ._proto import track_pb2 as proto_track
from ._proto import stats_pb2 as proto_stats

if TYPE_CHECKING:
    from .audio_source import AudioSource
    from .video_source import VideoSource


@dataclass(frozen=True)
class TrackPublishTimingEvent:
    track_handle: int
    stage: proto_track.PublishTimingStage.ValueType
    timestamp_us: int
    capture_timestamp_us: int
    frame_id: Optional[int]
    rtp_timestamp: int
    ssrc: int


class Track:
    def __init__(self, owned_info: proto_track.OwnedTrack):
        self._info = owned_info.info
        self._ffi_handle = FfiHandle(owned_info.handle.id)

    @property
    def sid(self) -> str:
        return self._info.sid

    @property
    def name(self) -> str:
        return self._info.name

    @property
    def kind(self) -> proto_track.TrackKind.ValueType:
        return self._info.kind

    @property
    def stream_state(self) -> proto_track.StreamState.ValueType:
        return self._info.stream_state

    @property
    def muted(self) -> bool:
        return self._info.muted

    async def get_stats(self) -> List[proto_stats.RtcStats]:
        req = proto_ffi.FfiRequest()
        req.get_stats.track_handle = self._ffi_handle.handle

        queue = FfiClient.instance.queue.subscribe()
        try:
            resp = FfiClient.instance.request(req)
            cb: proto_ffi.FfiEvent = await queue.wait_for(
                lambda e: e.get_stats.async_id == resp.get_stats.async_id
            )
        finally:
            FfiClient.instance.queue.unsubscribe(queue)

        if cb.get_stats.error:
            raise Exception(cb.get_stats.error)

        return list(cb.get_stats.stats)


class LocalAudioTrack(Track):
    def __init__(self, info: proto_track.OwnedTrack):
        super().__init__(info)

    @staticmethod
    def create_audio_track(name: str, source: "AudioSource") -> "LocalAudioTrack":
        req = proto_ffi.FfiRequest()
        req.create_audio_track.name = name
        req.create_audio_track.source_handle = source._ffi_handle.handle

        resp = FfiClient.instance.request(req)
        return LocalAudioTrack(resp.create_audio_track.track)

    def mute(self):
        req = proto_ffi.FfiRequest()
        req.local_track_mute.track_handle = self._ffi_handle.handle
        req.local_track_mute.mute = True
        FfiClient.instance.request(req)
        self._info.muted = True

    def unmute(self):
        req = proto_ffi.FfiRequest()
        req.local_track_mute.track_handle = self._ffi_handle.handle
        req.local_track_mute.mute = False
        FfiClient.instance.request(req)
        self._info.muted = False

    async def publish_timing_events(self) -> AsyncIterator[TrackPublishTimingEvent]:
        """Observe RTP publish timing for this local audio track.

        Events are emitted by the native sender path after audio frames have
        reached WebRTC packetization, and include the RTP timestamp/SSRC needed
        to correlate published media time with receiver playout time.
        """

        handle = self._ffi_handle.handle
        queue = FfiClient.instance.queue.subscribe(
            filter_fn=lambda event: (
                event.WhichOneof("message") == "track_publish_timing"
                and event.track_publish_timing.track_handle == handle
            )
        )
        try:
            req = proto_ffi.FfiRequest()
            req.observe_track_publish_timing.track_handle = handle
            FfiClient.instance.request(req)

            while True:
                event = await queue.get()
                timing = event.track_publish_timing
                yield TrackPublishTimingEvent(
                    track_handle=timing.track_handle,
                    stage=timing.stage,
                    timestamp_us=timing.timestamp_us,
                    capture_timestamp_us=timing.capture_timestamp_us,
                    frame_id=timing.frame_id if timing.HasField("frame_id") else None,
                    rtp_timestamp=timing.rtp_timestamp,
                    ssrc=timing.ssrc,
                )
                queue.task_done()
        finally:
            FfiClient.instance.queue.unsubscribe(queue)

    def __repr__(self) -> str:
        return f"rtc.LocalAudioTrack(sid={self.sid}, name={self.name})"


class LocalVideoTrack(Track):
    def __init__(self, info: proto_track.OwnedTrack):
        super().__init__(info)

    @staticmethod
    def create_video_track(name: str, source: "VideoSource") -> "LocalVideoTrack":
        req = proto_ffi.FfiRequest()
        req.create_video_track.name = name
        req.create_video_track.source_handle = source._ffi_handle.handle

        resp = FfiClient.instance.request(req)
        return LocalVideoTrack(resp.create_video_track.track)

    def mute(self):
        req = proto_ffi.FfiRequest()
        req.local_track_mute.track_handle = self._ffi_handle.handle
        req.local_track_mute.mute = True
        FfiClient.instance.request(req)
        self._info.muted = True

    def unmute(self):
        req = proto_ffi.FfiRequest()
        req.local_track_mute.track_handle = self._ffi_handle.handle
        req.local_track_mute.mute = False
        FfiClient.instance.request(req)
        self._info.muted = False

    def __repr__(self) -> str:
        return f"rtc.LocalVideoTrack(sid={self.sid}, name={self.name})"


class RemoteAudioTrack(Track):
    def __init__(self, info: proto_track.OwnedTrack):
        super().__init__(info)

    def __repr__(self) -> str:
        return f"rtc.RemoteAudioTrack(sid={self.sid}, name={self.name})"


class RemoteVideoTrack(Track):
    def __init__(self, info: proto_track.OwnedTrack):
        super().__init__(info)

    def __repr__(self) -> str:
        return f"rtc.RemoteVideoTrack(sid={self.sid}, name={self.name})"


LocalTrack = Union[LocalVideoTrack, LocalAudioTrack]
RemoteTrack = Union[RemoteVideoTrack, RemoteAudioTrack]
AudioTrack = Union[LocalAudioTrack, RemoteAudioTrack]
VideoTrack = Union[LocalVideoTrack, RemoteVideoTrack]
