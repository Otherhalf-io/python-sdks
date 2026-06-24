"""Smoke test: import the SDK and initialize the FFI library."""


def test_import_and_ffi_initialize():
    from livekit import rtc  # noqa: F401
    from livekit.rtc._ffi_client import FfiClient

    # accessing .instance triggers livekit_ffi_initialize
    assert FfiClient.instance is not None


def test_publish_timing_api_is_exported():
    from livekit import rtc

    assert rtc.PublishTimingStage.PUBLISH_TIMING_STAGE_WEBRTC_PACKETIZE == 2
    assert rtc.TrackPublishTimingEvent.__name__ == "TrackPublishTimingEvent"
    assert hasattr(rtc.LocalAudioTrack, "publish_timing_events")
