package mpv

// Audio playback using a single, persistent mpv media server. See mpv.io
// https://github.com/dexterlb/mpvipc
// https://mpv.io/manual/master/#json-ipc
// https://mpv.io/manual/master/#properties
//
// MpvPlayer keeps one mpv process idle and drives it over the JSON IPC. It holds
// a rolling window of [current, next] in mpv's own playlist, so mpv's built-in
// --gapless-audio bridges the transition between tracks with no gap. When mpv
// advances to the pre-loaded next entry, the onAdvance callback fires so the
// owning device can advance its queue and pre-load the following track. If mpv
// unexpectedly runs out of playlist (idle) or its process dies, onStalled /
// onExit let the device recover.

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/dexterlb/mpvipc"
	"github.com/navidrome/navidrome/log"
	"github.com/navidrome/navidrome/model"
)

// observe_property id. idle-active flips true when mpv runs out of playlist
// (the "playback halted" signature we recover from). Gapless advances are
// detected from mpv's start-file event, not playlist-pos: SetNext trims played
// entries, which renumbers playlist-pos, and mpv does not reliably emit that
// change - so a pos-delta detector goes stale and misses advances. start-file
// fires on every entry that actually begins playing, which is exactly an
// advance (the only other start-file is the current track we load ourselves).
const (
	idleActivePropertyID = 2
)

type MpvPlayer struct {
	conn          *mpvipc.Connection
	exe           *Executor
	ipcSocketName string
	onAdvance     func()
	onStalled     func()
	onExit        func()
	closeCalled   atomic.Bool
	// loadGeneration is bumped on every explicit (re)load (Play). listen() uses
	// it to discard playlist-pos events buffered from a superseded playlist,
	// which would otherwise look like spurious advances after a Skip/Set.
	loadGeneration atomic.Int64
	stopListener   chan struct{}
}

// NewPlayer starts a single idle mpv process for the device and opens its IPC
// connection. onAdvance fires when mpv gaplessly moves to the pre-loaded next
// track; onStalled fires when mpv unexpectedly goes idle (playlist exhausted);
// onExit fires if mpv dies. Its signature matches how playback.PlayerFactory
// unpacks its callbacks.
func NewPlayer(ctx context.Context, deviceName string, onAdvance, onStalled, onExit func()) (*MpvPlayer, error) {
	if _, err := mpvCommand(); err != nil {
		return nil, err
	}

	tmpSocketName := socketName("mpv-ctrl-", ".socket")

	args := createMPVCommand(deviceName, tmpSocketName)
	if len(args) == 0 {
		return nil, fmt.Errorf("no mpv command arguments provided")
	}
	exe, err := start(ctx, args)
	if err != nil {
		log.Error("Error starting mpv process", err)
		return nil, err
	}
	// Drain mpv's stdout for the life of the process. mpv logs its terminal
	// status line (position, cache, etc.) continuously and nothing here reads
	// it; because the Executor wires stdout to a synchronous io.Pipe, an unread
	// write eventually blocks mpv's playloop mid-write (seen as an
	// anon_pipe_write hang that froze IPC and gapless advance after a few
	// tracks). We drive all state over the IPC, so this output is pure noise -
	// discard it. --no-terminal in the command keeps the volume near zero; this
	// is the belt-and-braces backstop.
	go func() { _, _ = io.Copy(io.Discard, exe.PipeReader) }()

	// wait for socket to show up
	if err := waitForSocket(tmpSocketName, 3*time.Second, 100*time.Millisecond); err != nil {
		log.Error("Error or timeout waiting for control socket", "socketname", tmpSocketName, err)
		return nil, err
	}

	conn := mpvipc.NewConnection(tmpSocketName)
	if err := conn.Open(); err != nil {
		log.Error("Error opening new connection", err)
		return nil, err
	}

	p := &MpvPlayer{
		conn:          conn,
		exe:           &exe,
		ipcSocketName: tmpSocketName,
		onAdvance:     onAdvance,
		onStalled:     onStalled,
		onExit:        onExit,
		stopListener:  make(chan struct{}),
	}

	// Watch idle-active (unexpected stop). Advances come from the start-file
	// event stream, which needs no observe_property registration.
	if _, err := conn.Call("observe_property", idleActivePropertyID, "idle-active"); err != nil {
		log.Warn("Could not observe mpv idle-active", err)
	}
	go p.listen()

	return p, nil
}

// listen watches mpv events: it invokes onAdvance on a gapless advance,
// onStalled when mpv unexpectedly idles, and onExit if mpv dies. It also logs
// the events that explain a halt (a failed track load).
func (p *MpvPlayer) listen() {
	events, stop := p.conn.NewEventListener()
	defer close(stop)
	seenGen := p.loadGeneration.Load()
	// currentStarted records whether we have seen the start-file for the track
	// we loaded ourselves in this generation. The first start-file after a
	// Play/Skip/Set is that current track (not an advance); every later one is
	// mpv gaplessly moving to the pre-loaded next entry (an advance).
	currentStarted := false
	for {
		select {
		case <-p.stopListener:
			return
		case e, ok := <-events:
			if !ok {
				// The connection closed. If we did not ask for it, mpv died.
				if !p.closeCalled.Load() {
					log.Warn("mpv exited unexpectedly")
					if p.onExit != nil {
						p.onExit()
					}
				} else {
					log.Debug("mpv event stream closed")
				}
				return
			}
			// A load (Play/Skip/Set) bumps the generation; the next start-file is
			// the freshly loaded current track, so re-arm the advance detector.
			if gen := p.loadGeneration.Load(); gen != seenGen {
				seenGen = gen
				currentStarted = false
			}
			switch e.Name {
			case "start-file":
				if !currentStarted {
					// The current track we just (re)loaded; not an advance.
					currentStarted = true
					log.Trace("mpv started the current track")
				} else if !p.closeCalled.Load() && p.onAdvance != nil {
					log.Debug("mpv advanced to next track")
					p.onAdvance()
				}
			case "property-change":
				if e.ID == idleActivePropertyID {
					if idle, _ := e.Data.(bool); idle && !p.closeCalled.Load() {
						// mpv has nothing left to play. Log the playlist shape so
						// we can tell an expected end-of-queue from a bug, then
						// let the device decide whether to recover.
						count, _ := toInt2(p.conn.Get("playlist-count"))
						pos, _ := toInt2(p.conn.Get("playlist-pos"))
						log.Debug("mpv became idle", "playlistPos", pos, "playlistCount", count)
						if p.onStalled != nil {
							p.onStalled()
						}
					}
				}
			case "end-file":
				// reason: eof, stop, quit, error, redirect, unknown
				if e.Reason == "error" || e.Reason == "unknown" {
					log.Warn("mpv could not play a track", "reason", e.Reason)
				} else {
					log.Trace("mpv finished a playlist entry", "reason", e.Reason)
				}
			}
		}
	}
}

func toInt(v any) (int, bool) {
	switch n := v.(type) {
	case float64:
		return int(n), true
	case int:
		return n, true
	case int64:
		return int(n), true
	default:
		return 0, false
	}
}

// toInt2 adapts a (value, error) Get result for logging.
func toInt2(v any, err error) (int, bool) {
	if err != nil {
		return 0, false
	}
	return toInt(v)
}

func (p *MpvPlayer) Play(current model.MediaFile, next *model.MediaFile) error {
	nextPath := "<none>"
	if next != nil {
		nextPath = next.Path
	}
	log.Debug("Playing track, pre-loading next", "current", current.Path, "next", nextPath)
	// A replace supersedes any buffered advance events from the old playlist.
	p.loadGeneration.Add(1)
	if _, err := p.conn.Call("loadfile", current.AbsolutePath(), "replace"); err != nil {
		log.Error("Error loading current track", "track", current.Path, err)
		return err
	}
	if next != nil {
		if _, err := p.conn.Call("loadfile", next.AbsolutePath(), "append"); err != nil {
			log.Warn("Could not pre-load next track", "track", next.Path, err)
		}
	}
	p.Unpause()
	return nil
}

// SetNext normalizes mpv's playlist to exactly [current, next], keeping the
// pre-loaded next in sync with the queue and bounding the playlist so it does
// not grow across a long session.
func (p *MpvPlayer) SetNext(next *model.MediaFile) {
	nextPath := "<none>"
	if next != nil {
		nextPath = next.Path
	}
	posRaw, posErr := p.conn.Get("playlist-pos")
	countRaw, countErr := p.conn.Get("playlist-count")
	if posErr != nil || countErr != nil {
		log.Warn("Could not read mpv playlist state; setting next without trimming", "next", nextPath, "posErr", posErr, "countErr", countErr)
	} else {
		pos, _ := toInt(posRaw)
		count, _ := toInt(countRaw)
		log.Debug("Setting next track", "next", nextPath, "playlistPos", pos, "playlistCount", count)
		if pos >= 0 {
			// Drop any stale pre-loaded entries after the current one.
			for i := count - 1; i > pos; i-- {
				p.removePlaylistEntry(i)
			}
			// Drop already-played entries before the current one so the playlist
			// stays bounded to [current, next]. Removing a lower index shifts the
			// current entry down; the resulting playlist-pos decrease is not a
			// forward advance, so it is ignored by listen().
			for i := pos - 1; i >= 0; i-- {
				p.removePlaylistEntry(i)
			}
		}
	}
	if next != nil {
		if _, err := p.conn.Call("loadfile", next.AbsolutePath(), "append"); err != nil {
			log.Warn("Could not set next track", "track", next.Path, err)
		}
	}
}

func (p *MpvPlayer) removePlaylistEntry(index int) {
	if _, err := p.conn.Call("playlist-remove", index); err != nil {
		log.Warn("Could not remove mpv playlist entry", "index", index, err)
	}
}

func (p *MpvPlayer) Clear() {
	p.Pause()
	if _, err := p.conn.Call("playlist-clear"); err != nil {
		log.Warn("Could not clear mpv playlist", err)
	}
}

func (p *MpvPlayer) SetVolume(value float32) {
	// mpv volume: 0 = silence, 100 = no reduction/amplification.
	log.Debug("Setting volume", "volume", value)
	vol := int(value * 100)
	if err := p.conn.Set("volume", vol); err != nil {
		log.Error("Error setting volume", "volume", value, err)
	}
}

func (p *MpvPlayer) Unpause() {
	log.Debug("Unpausing")
	if err := p.conn.Set("pause", false); err != nil {
		log.Error("Error unpausing", err)
	}
}

func (p *MpvPlayer) Pause() {
	log.Debug("Pausing")
	if err := p.conn.Set("pause", true); err != nil {
		log.Error("Error pausing", err)
	}
}

func (p *MpvPlayer) Close() {
	log.Debug("Closing mpv player")
	p.closeCalled.Store(true)
	select {
	case <-p.stopListener:
	default:
		close(p.stopListener)
	}
	if p.isSocketFilePresent() {
		log.Debug("sending shutdown command")
		if _, err := p.conn.Call("quit"); err != nil {
			log.Warn("Error sending quit command to mpv-ipc socket", err)
			if p.exe != nil {
				if err := p.exe.Cancel(); err != nil {
					log.Warn("Error canceling executor", err)
				}
			}
		}
	}
	if p.isSocketFilePresent() {
		removeSocket(p.ipcSocketName)
	}
}

func (p *MpvPlayer) isSocketFilePresent() bool {
	if len(p.ipcSocketName) < 1 {
		return false
	}
	fileInfo, err := os.Stat(p.ipcSocketName)
	return err == nil && fileInfo != nil && !fileInfo.IsDir()
}

// Position returns the playback position in seconds within the current track.
// Every now and then the mpv IPC interface returns "mpv error: property
// unavailable"; in that case we retry.
func (p *MpvPlayer) Position() int {
	retryCount := 0
	for {
		position, err := p.conn.Get("time-pos")
		if err != nil && err.Error() == "mpv error: property unavailable" {
			retryCount += 1
			log.Debug("Got mpv error, retrying...", "retries", retryCount, err)
			if retryCount > 5 {
				return 0
			}
			time.Sleep(time.Duration(retryCount) * time.Millisecond)
			continue
		}
		if err != nil {
			log.Error("Error getting position", err)
			return 0
		}
		pos, ok := position.(float64)
		if !ok {
			log.Error("Could not cast position from mpv into float64", "position", position)
			return 0
		}
		return int(pos)
	}
}

func (p *MpvPlayer) SetPosition(offset int) error {
	log.Debug("Setting position", "offset", offset)
	if p.Position() == offset {
		return nil
	}
	// Immediately after a (re)load - e.g. a Skip with an offset - mpv answers
	// "property unavailable" until the new file is loaded far enough to seek.
	// Retry briefly (Position() does the same for reads) so the seek lands
	// instead of failing the whole jukeboxControl request. ~1s covers a local
	// file's load; if it never becomes seekable we surface the error.
	var err error
	for i := 0; i < 50; i++ {
		if err = p.conn.Set("time-pos", float64(offset)); err == nil {
			return nil
		}
		if err.Error() != "mpv error: property unavailable" {
			log.Error("Could not set the position", "offset", offset, err)
			return err
		}
		time.Sleep(20 * time.Millisecond)
	}
	log.Warn("Could not set the position; track not seekable in time", "offset", offset, err)
	return err
}

// IsPlaying reports whether mpv is actively playing (not paused, buffering or
// idle at the end of the playlist).
func (p *MpvPlayer) IsPlaying() bool {
	idle, err := p.conn.Get("core-idle")
	if err != nil {
		log.Error("Problem getting core-idle status", err)
		return false
	}
	coreIdle, ok := idle.(bool)
	if !ok {
		return false
	}
	return !coreIdle
}

// IsIdle reports whether mpv has no track loaded (playlist exhausted), as
// opposed to merely paused. Used to tell a resume from a needed reload.
func (p *MpvPlayer) IsIdle() bool {
	idle, err := p.conn.Get("idle-active")
	if err != nil {
		return false
	}
	active, _ := idle.(bool)
	return active
}

func waitForSocket(path string, timeout time.Duration, pause time.Duration) error {
	start := time.Now()
	end := start.Add(timeout)
	retries := 0
	for {
		fileInfo, err := os.Stat(path)
		if err == nil && fileInfo != nil && !fileInfo.IsDir() {
			log.Debug("Socket found", "retries", retries, "waitTime", time.Since(start))
			return nil
		}
		if time.Now().After(end) {
			return fmt.Errorf("timeout reached: %s", timeout)
		}
		time.Sleep(pause)
		retries += 1
	}
}
