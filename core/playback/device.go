package playback

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/navidrome/navidrome/core/playback/mpv"
	"github.com/navidrome/navidrome/log"
	"github.com/navidrome/navidrome/model"
)

// defaultPlayerFactory adapts mpv.NewPlayer (which returns the concrete
// *mpv.MpvPlayer) to the Player interface. It lives here so the mpv package
// never needs to import this one.
func defaultPlayerFactory(ctx context.Context, deviceName string, cb PlayerCallbacks) (Player, error) {
	return mpv.NewPlayer(ctx, deviceName, cb.OnAdvance, cb.OnStalled, cb.OnExit)
}

type playbackDevice struct {
	serviceCtx           context.Context
	ParentPlaybackServer PlaybackServer
	Default              bool
	User                 string
	Name                 string
	DeviceName           string
	PlaybackQueue        *Queue
	Gain                 float32
	PlaybackDone         chan bool
	// mu guards the mutable device state (queue, player, loadedIndex, gain),
	// which is touched both by concurrent Subsonic HTTP handlers and by the
	// trackSwitcher goroutine on a gapless advance.
	mu sync.Mutex
	// newPlayer builds the audio backend (injectable for tests).
	newPlayer PlayerFactory
	// player is a single backend reused for the whole device lifetime; fed a
	// rolling [current, next] window so gapless-capable backends bridge tracks.
	player Player
	// loadedIndex is the queue index the player currently has loaded, or -1.
	loadedIndex        int
	startTrackSwitcher sync.Once
}

type DeviceStatus struct {
	CurrentIndex int
	Playing      bool
	Gain         float32
	Position     int
}

const DefaultGain float32 = 1.0

// getStatus returns the full status, including the IPC-derived fields. Assumes
// pd.mu is held (so it does IPC under the lock - fine for the infrequent action
// methods; the frequently-polled Status/Get use statusLocked+completeStatus to
// keep the IPC off the lock).
func (pd *playbackDevice) getStatus() DeviceStatus {
	return completeStatus(pd.player, pd.statusLocked())
}

// statusLocked returns the status fields known without IPC. Assumes pd.mu held.
func (pd *playbackDevice) statusLocked() DeviceStatus {
	return DeviceStatus{
		CurrentIndex: pd.PlaybackQueue.Index,
		Gain:         pd.Gain,
	}
}

// completeStatus fills the IPC-derived fields (Playing, Position). It must be
// called WITHOUT pd.mu held so status polls don't serialize on backend IPC.
func completeStatus(p Player, st DeviceStatus) DeviceStatus {
	if p != nil {
		st.Playing = p.IsPlaying()
		st.Position = p.Position()
	}
	return st
}

// NewPlaybackDevice creates a new playback device which implements all the basic Jukebox mode commands defined here:
// http://www.subsonic.org/pages/api.jsp#jukeboxControl
func NewPlaybackDevice(ctx context.Context, playbackServer PlaybackServer, name string, deviceName string) *playbackDevice {
	return &playbackDevice{
		serviceCtx:           ctx,
		ParentPlaybackServer: playbackServer,
		User:                 "",
		Name:                 name,
		DeviceName:           deviceName,
		Gain:                 DefaultGain,
		PlaybackQueue:        NewQueue(),
		PlaybackDone:         make(chan bool),
		newPlayer:            defaultPlayerFactory,
		loadedIndex:          -1,
	}
}

func (pd *playbackDevice) String() string {
	return fmt.Sprintf("Name: %s, Gain: %.4f, LoadedIndex: %d", pd.Name, pd.Gain, pd.loadedIndex)
}

func (pd *playbackDevice) Get(ctx context.Context) (model.MediaFiles, DeviceStatus, error) {
	pd.mu.Lock()
	log.Debug(ctx, "Processing Get action", "device", pd)
	items, st, p := pd.PlaybackQueue.Get(), pd.statusLocked(), pd.player
	pd.mu.Unlock()
	return items, completeStatus(p, st), nil
}

func (pd *playbackDevice) Status(ctx context.Context) (DeviceStatus, error) {
	pd.mu.Lock()
	log.Debug(ctx, fmt.Sprintf("processing Status action on: %s, queue: %s", pd, pd.PlaybackQueue))
	st, p := pd.statusLocked(), pd.player
	pd.mu.Unlock()
	return completeStatus(p, st), nil
}

// Set replaces the queue (a Clear followed by an Add). Playback is stopped; the
// new queue is not started until a subsequent Start or Skip.
func (pd *playbackDevice) Set(ctx context.Context, ids []string) (DeviceStatus, error) {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	log.Debug(ctx, "Processing Set action", "ids", ids, "device", pd)

	if _, err := pd.clearLocked(ctx); err != nil {
		log.Error(ctx, "error setting tracks", ids)
		return pd.getStatus(), err
	}
	return pd.addLocked(ctx, ids)
}

func (pd *playbackDevice) Start(ctx context.Context) (DeviceStatus, error) {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	log.Debug(ctx, "Processing Start action", "device", pd)

	if pd.PlaybackQueue.IsEmpty() {
		return pd.getStatus(), nil
	}

	if pd.isPlaying() {
		log.Debug("trying to start an already playing track")
		return pd.getStatus(), nil
	}

	// Resume a track that is loaded and merely paused. If the backend is idle
	// (nothing loaded - e.g. it was stopped or died), fall through and reload.
	if pd.player != nil && pd.loadedIndex == pd.PlaybackQueue.Index && pd.loadedIndex >= 0 && !pd.player.IsIdle() {
		pd.player.Unpause()
		return pd.getStatus(), nil
	}

	if err := pd.switchActiveTrackByIndex(pd.PlaybackQueue.Index); err != nil {
		return pd.getStatus(), err
	}
	return pd.getStatus(), nil
}

func (pd *playbackDevice) Stop(ctx context.Context) (DeviceStatus, error) {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	return pd.stopLocked(ctx)
}

func (pd *playbackDevice) stopLocked(ctx context.Context) (DeviceStatus, error) {
	log.Debug(ctx, "Processing Stop action", "device", pd)
	if pd.player != nil {
		pd.player.Pause()
	}
	return pd.getStatus(), nil
}

func (pd *playbackDevice) Skip(ctx context.Context, index int, offset int) (DeviceStatus, error) {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	log.Debug(ctx, "Processing Skip action", "index", index, "offset", offset, "device", pd)

	// A skip to the already-loaded track is a seek - just reposition it. Any
	// other target (or an idle/empty backend) loads and plays that track.
	if pd.player != nil && !pd.player.IsIdle() && index == pd.loadedIndex {
		pd.PlaybackQueue.SetIndex(index)
	} else if err := pd.switchActiveTrackByIndex(index); err != nil {
		return pd.getStatus(), err
	}

	if offset > 0 {
		if err := pd.player.SetPosition(offset); err != nil {
			log.Error(ctx, "error setting position", err)
			return pd.getStatus(), err
		}
	}

	// A skip always (re)starts playback: clients such as DSub use it to begin an
	// album from a stopped state (after the previous queue ended).
	pd.player.Unpause()

	return pd.getStatus(), nil
}

func (pd *playbackDevice) Add(ctx context.Context, ids []string) (DeviceStatus, error) {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	return pd.addLocked(ctx, ids)
}

func (pd *playbackDevice) addLocked(ctx context.Context, ids []string) (DeviceStatus, error) {
	log.Debug(ctx, "Processing Add action", "ids", ids, "device", pd)
	if len(ids) < 1 {
		return pd.getStatus(), nil
	}

	items := model.MediaFiles{}

	for _, id := range ids {
		mf, err := pd.ParentPlaybackServer.GetMediaFile(id)
		if err != nil {
			return DeviceStatus{}, err
		}
		log.Debug(ctx, "Found mediafile: "+mf.Path)
		items = append(items, *mf)
	}
	pd.PlaybackQueue.Add(items)

	// Keep the player's pre-loaded "next" in sync with the (possibly new) tail.
	pd.refreshNext()

	return pd.getStatus(), nil
}

func (pd *playbackDevice) Clear(ctx context.Context) (DeviceStatus, error) {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	return pd.clearLocked(ctx)
}

func (pd *playbackDevice) clearLocked(ctx context.Context) (DeviceStatus, error) {
	log.Debug(ctx, "Processing Clear action", "device", pd)
	if pd.player != nil {
		pd.player.Clear()
	}
	pd.loadedIndex = -1
	pd.PlaybackQueue.Clear()
	return pd.getStatus(), nil
}

func (pd *playbackDevice) Remove(ctx context.Context, index int) (DeviceStatus, error) {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	log.Debug(ctx, "Processing Remove action", "index", index, "device", pd)
	// pausing if attempting to remove running track
	removingCurrent := pd.PlaybackQueue.Index == index
	if pd.isPlaying() && removingCurrent {
		if _, err := pd.stopLocked(ctx); err != nil {
			log.Error(ctx, "error stopping running track")
			return pd.getStatus(), err
		}
	}

	if index > -1 && index < pd.PlaybackQueue.Size() {
		pd.PlaybackQueue.Remove(index)
	} else {
		log.Error(ctx, "Index to remove out of range: "+fmt.Sprint(index))
		return pd.getStatus(), nil
	}

	if removingCurrent {
		// The loaded track is gone; force a reload on the next Start.
		pd.loadedIndex = -1
	} else {
		pd.refreshNext()
	}
	return pd.getStatus(), nil
}

func (pd *playbackDevice) Shuffle(ctx context.Context) (DeviceStatus, error) {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	log.Debug(ctx, "Processing Shuffle action", "device", pd)
	if pd.PlaybackQueue.Size() > 1 {
		pd.PlaybackQueue.Shuffle()
	}
	pd.loadedIndex = pd.PlaybackQueue.Index
	pd.refreshNext()
	return pd.getStatus(), nil
}

// SetGain is used to control the playback volume. A float value between 0.0 and 1.0.
func (pd *playbackDevice) SetGain(ctx context.Context, gain float32) (DeviceStatus, error) {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	log.Debug(ctx, "Processing SetGain action", "newGain", gain, "device", pd)

	if pd.player != nil {
		pd.player.SetVolume(gain)
	}
	pd.Gain = gain

	return pd.getStatus(), nil
}

// isPlaying assumes pd.mu is held.
func (pd *playbackDevice) isPlaying() bool {
	return pd.player != nil && pd.player.IsPlaying()
}

// ensurePlayer lazily creates the single backend and starts the trackSwitcher
// goroutine that consumes gapless-advance signals. Assumes pd.mu is held.
func (pd *playbackDevice) ensurePlayer() error {
	if pd.player == nil {
		p, err := pd.newPlayer(pd.serviceCtx, pd.DeviceName, PlayerCallbacks{
			OnAdvance: func() {
				// Runs on the backend's goroutine; hand off to the single
				// trackSwitcher goroutine to serialize queue mutations. Guard
				// against shutdown so a late advance cannot block the backend's
				// goroutine forever.
				select {
				case pd.PlaybackDone <- true:
				case <-pd.serviceCtx.Done():
				}
			},
			OnStalled: pd.handleStalled,
			OnExit:    pd.handlePlayerExit,
		})
		if err != nil {
			return err
		}
		pd.player = p
		pd.player.SetVolume(pd.Gain)
	}
	pd.startTrackSwitcher.Do(func() {
		log.Debug("Starting trackSwitcher goroutine")
		go pd.trackSwitcherGoroutine()
	})
	return nil
}

// nextMediaFile returns the track after the current queue index, or nil.
// Assumes pd.mu is held.
func (pd *playbackDevice) nextMediaFile() *model.MediaFile {
	items := pd.PlaybackQueue.Get()
	ni := pd.PlaybackQueue.Index + 1
	if ni >= 0 && ni < len(items) {
		return &items[ni]
	}
	return nil
}

// refreshNext re-syncs the backend's pre-loaded next entry after a queue change.
// Assumes pd.mu is held.
func (pd *playbackDevice) refreshNext() {
	if pd.player != nil && pd.loadedIndex >= 0 {
		pd.player.SetNext(pd.nextMediaFile())
	}
}

// handlePlayerExit resets the backend after it dies unexpectedly (e.g. mpv
// crashed), so the next playback command recreates it. Runs on the backend's
// goroutine.
func (pd *playbackDevice) handlePlayerExit() {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	log.Warn("Jukebox backend exited unexpectedly; it will be recreated on the next playback command")
	pd.player = nil
	pd.loadedIndex = -1
}

// handleStalled recovers when the backend stopped with tracks still queued
// (e.g. a track failed to load, so gapless had nothing to bridge to): advance
// to and hard-reload the next track. A genuine end-of-queue is left stopped.
// Runs on the backend's goroutine.
func (pd *playbackDevice) handleStalled() {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	if pd.player == nil || pd.loadedIndex < 0 {
		return
	}
	if pd.PlaybackQueue.IsAtLastElement() {
		log.Debug("Jukebox reached end of queue")
		return
	}
	log.Warn("Jukebox backend stalled with tracks remaining; recovering", "index", pd.PlaybackQueue.Index)
	pd.PlaybackQueue.IncreaseIndex()
	if err := pd.switchActiveTrackByIndex(pd.PlaybackQueue.Index); err != nil {
		log.Error("Failed to recover stalled jukebox playback", err)
	}
}

func (pd *playbackDevice) trackSwitcherGoroutine() {
	log.Debug("Started trackSwitcher goroutine", "device", pd)
	for {
		select {
		case <-pd.PlaybackDone:
			pd.handleAdvance()
		case <-pd.serviceCtx.Done():
			log.Debug("Stopping trackSwitcher goroutine", "device", pd.Name)
			pd.mu.Lock()
			if pd.player != nil {
				pd.player.Close()
			}
			pd.mu.Unlock()
			return
		}
	}
}

// handleAdvance reacts to a gapless advance: the backend already moved on to
// the pre-loaded next track, so advance our own queue index and pre-load the
// following track to keep the lookahead full.
func (pd *playbackDevice) handleAdvance() {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	from := pd.PlaybackQueue.Index
	if pd.PlaybackQueue.IsAtLastElement() {
		log.Debug("Reached end of jukebox queue", "index", from, "size", pd.PlaybackQueue.Size())
		return
	}
	pd.PlaybackQueue.IncreaseIndex()
	pd.loadedIndex = pd.PlaybackQueue.Index
	next := pd.nextMediaFile()
	nextPath := "<none>"
	if next != nil {
		nextPath = next.Path
	}
	log.Debug("Advancing jukebox queue", "fromIndex", from, "toIndex", pd.PlaybackQueue.Index, "size", pd.PlaybackQueue.Size(), "newNext", nextPath)
	if pd.player != nil {
		pd.player.SetNext(next)
		pd.player.SetVolume(pd.Gain)
	}
}

// switchActiveTrackByIndex loads the track at index as the current entry and
// pre-loads the following one, starting playback. Assumes pd.mu is held.
func (pd *playbackDevice) switchActiveTrackByIndex(index int) error {
	pd.PlaybackQueue.SetIndex(index)
	current := pd.PlaybackQueue.Current()
	if current == nil {
		return errors.New("could not get current track")
	}

	if err := pd.ensurePlayer(); err != nil {
		return err
	}

	if err := pd.player.Play(*current, pd.nextMediaFile()); err != nil {
		return err
	}
	pd.player.SetVolume(pd.Gain)
	pd.loadedIndex = pd.PlaybackQueue.Index
	return nil
}
