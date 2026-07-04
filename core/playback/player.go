package playback

import (
	"context"

	"github.com/navidrome/navidrome/model"
)

// Player is the audio backend for a single playback device. The device owns the
// queue and only ever tells a Player about the current track and the one that
// should play next, so a backend that supports gapless playback (mpv, via
// SetNext pre-loading) can transition between them without a gap - and other
// sinks (e.g. DLNA) can implement the same contract. One Player is created per
// device and reused for its whole lifetime.
//
// The interface is defined here, next to its only consumer (playbackDevice);
// implementations satisfy it structurally and need not import this package.
type Player interface {
	// Play starts playing current immediately and pre-loads next (which may be
	// nil) so the transition to it is gapless. It replaces whatever was playing.
	Play(current model.MediaFile, next *model.MediaFile) error

	// SetNext updates the pre-loaded "next" track (nil clears it) after the
	// queue changed or after an advance, keeping exactly one track of lookahead.
	SetNext(next *model.MediaFile)

	// Clear stops playback and discards any loaded/queued media.
	Clear()

	Pause()
	Unpause()

	// IsPlaying reports whether audio is actively being produced (not paused
	// and not idle at the end of the queue).
	IsPlaying() bool

	// IsIdle reports whether the backend has no track loaded (as opposed to
	// merely paused), so the device knows to reload rather than resume.
	IsIdle() bool

	// Position/SetPosition operate on the currently playing track, in seconds.
	Position() int
	SetPosition(offset int) error

	// SetVolume sets the playback volume, 0.0-1.0.
	SetVolume(value float32)

	// Close shuts the backend down and releases its resources.
	Close()
}

// PlayerCallbacks let a Player report state transitions the device must react
// to. Any of them may run on an internal goroutine.
type PlayerCallbacks struct {
	// OnAdvance fires when the Player moves from the current track to the
	// pre-loaded next one, so the device advances its queue and supplies the
	// following track via SetNext.
	OnAdvance func()
	// OnStalled fires when the Player unexpectedly stops with tracks still
	// queued (e.g. a track failed to load), so the device can recover.
	OnStalled func()
	// OnExit fires if the backend dies, so the device recreates it on the next
	// playback command.
	OnExit func()
}

// PlayerFactory constructs a Player for a device.
type PlayerFactory func(ctx context.Context, deviceName string, cb PlayerCallbacks) (Player, error)
