package playback

import (
	"context"

	"github.com/navidrome/navidrome/model"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// fakePlayer is an in-memory Player implementation for testing the device's
// queue logic without a real mpv process. It records what it was asked to play
// and pre-load, and exposes the onAdvance callback so tests can simulate a
// gapless advance.
type fakePlayer struct {
	onAdvance func()
	onStalled func()
	onExit    func()
	current   *model.MediaFile
	next      *model.MediaFile
	playing   bool
	idle      bool
	gain      float32
	closed    bool
}

func (f *fakePlayer) Play(current model.MediaFile, next *model.MediaFile) error {
	c := current
	f.current = &c
	f.next = next
	f.playing = true
	f.idle = false
	return nil
}
func (f *fakePlayer) SetNext(next *model.MediaFile) { f.next = next }
func (f *fakePlayer) Clear()                        { f.playing = false; f.current = nil; f.next = nil }
func (f *fakePlayer) Pause()                        { f.playing = false }
func (f *fakePlayer) Unpause()                      { f.playing = true }
func (f *fakePlayer) IsPlaying() bool               { return f.playing && !f.idle }
func (f *fakePlayer) IsIdle() bool                  { return f.idle }
func (f *fakePlayer) Position() int                 { return 0 }
func (f *fakePlayer) SetPosition(offset int) error  { return nil }
func (f *fakePlayer) SetVolume(value float32)       { f.gain = value }
func (f *fakePlayer) Close()                        { f.closed = true }

// fakePlaybackServer serves MediaFiles by id for Add/Set.
type fakePlaybackServer struct{ files map[string]model.MediaFile }

func (s *fakePlaybackServer) Run(ctx context.Context) error                         { return nil }
func (s *fakePlaybackServer) GetDeviceForUser(user string) (*playbackDevice, error) { return nil, nil }
func (s *fakePlaybackServer) GetMediaFile(id string) (*model.MediaFile, error) {
	mf := s.files[id]
	return &mf, nil
}

var _ = Describe("playbackDevice", func() {
	var (
		pd     *playbackDevice
		fake   *fakePlayer
		ctx    context.Context
		cancel context.CancelFunc
	)

	BeforeEach(func() {
		ctx, cancel = context.WithCancel(context.Background())
		srv := &fakePlaybackServer{files: map[string]model.MediaFile{
			"1": {ID: "1", Path: "/m/a.mp3"},
			"2": {ID: "2", Path: "/m/b.mp3"},
			"3": {ID: "3", Path: "/m/c.mp3"},
		}}
		pd = NewPlaybackDevice(ctx, srv, "test", "test")
		fake = &fakePlayer{}
		// Inject the fake backend instead of mpv.
		pd.newPlayer = func(_ context.Context, _ string, cb PlayerCallbacks) (Player, error) {
			fake.onAdvance = cb.OnAdvance
			fake.onStalled = cb.OnStalled
			fake.onExit = cb.OnExit
			return fake, nil
		}
	})

	AfterEach(func() { cancel() })

	It("loads the current track and pre-loads the next one on Start", func() {
		_, err := pd.Set(ctx, []string{"1", "2", "3"})
		Expect(err).ToNot(HaveOccurred())

		_, err = pd.Start(ctx)
		Expect(err).ToNot(HaveOccurred())

		Expect(fake.current).ToNot(BeNil())
		Expect(fake.current.ID).To(Equal("1"))
		Expect(fake.next).ToNot(BeNil())
		Expect(fake.next.ID).To(Equal("2"))
		Expect(pd.PlaybackQueue.Index).To(Equal(0))
		Expect(pd.isPlaying()).To(BeTrue())
	})

	It("advances the queue and pre-loads the following track on a gapless advance", func() {
		_, _ = pd.Set(ctx, []string{"1", "2", "3"})
		_, _ = pd.Start(ctx)

		// mpv would fire this when it gaplessly moves to the pre-loaded next.
		pd.handleAdvance()

		Expect(pd.PlaybackQueue.Index).To(Equal(1))
		Expect(fake.next).ToNot(BeNil())
		Expect(fake.next.ID).To(Equal("3")) // the new lookahead
	})

	It("clears the pre-loaded next at the end of the queue", func() {
		_, _ = pd.Set(ctx, []string{"1", "2", "3"})
		_, _ = pd.Start(ctx)
		pd.handleAdvance() // 0 -> 1
		pd.handleAdvance() // 1 -> 2 (last); no next to pre-load

		Expect(pd.PlaybackQueue.Index).To(Equal(2))
		Expect(fake.next).To(BeNil())
	})

	It("does not advance past the last track", func() {
		_, _ = pd.Set(ctx, []string{"1", "2"})
		_, _ = pd.Start(ctx)
		pd.handleAdvance() // 0 -> 1 (last)
		pd.handleAdvance() // stays at 1

		Expect(pd.PlaybackQueue.Index).To(Equal(1))
	})

	It("resumes in place on Start after a pause instead of reloading", func() {
		_, _ = pd.Set(ctx, []string{"1", "2"})
		_, _ = pd.Start(ctx)
		_, _ = pd.Stop(ctx)
		Expect(pd.isPlaying()).To(BeFalse())

		fake.current = nil // detect an unwanted reload
		_, _ = pd.Start(ctx)

		Expect(pd.isPlaying()).To(BeTrue())
		Expect(fake.current).To(BeNil()) // resumed, not re-Played
	})

	It("recreates the backend after it exits unexpectedly", func() {
		_, _ = pd.Set(ctx, []string{"1", "2"})
		_, _ = pd.Start(ctx)
		Expect(fake.onExit).ToNot(BeNil())

		// Simulate the backend (mpv) dying.
		fake.onExit()

		// The dead backend was dropped; a new Start rebuilds and plays again.
		fake.current = nil
		_, err := pd.Start(ctx)
		Expect(err).ToNot(HaveOccurred())
		Expect(fake.current).ToNot(BeNil())
		Expect(fake.current.ID).To(Equal("1"))
	})

	It("recovers by advancing and reloading when the backend stalls mid-queue", func() {
		_, _ = pd.Set(ctx, []string{"1", "2", "3"})
		_, _ = pd.Start(ctx)
		Expect(fake.onStalled).ToNot(BeNil())

		// Simulate mpv going idle with tracks still queued (e.g. a track failed
		// to load so gapless had nothing to bridge to).
		fake.current = nil
		fake.onStalled()

		Expect(pd.PlaybackQueue.Index).To(Equal(1))
		Expect(fake.current).ToNot(BeNil()) // hard-reloaded the next track
		Expect(fake.current.ID).To(Equal("2"))
	})

	It("stays stopped when the backend stalls at the end of the queue", func() {
		_, _ = pd.Set(ctx, []string{"1"})
		_, _ = pd.Start(ctx)

		fake.current = nil
		fake.onStalled() // at the last element: a genuine end-of-queue

		Expect(pd.PlaybackQueue.Index).To(Equal(0))
		Expect(fake.current).To(BeNil()) // no reload
	})

	It("reloads instead of resuming when the backend is idle on Start", func() {
		_, _ = pd.Set(ctx, []string{"1", "2"})
		_, _ = pd.Start(ctx)

		// The backend went idle (stopped/ran out) at the same index; Start must
		// reload rather than issue a no-op Unpause.
		fake.idle = true
		fake.current = nil
		_, _ = pd.Start(ctx)

		Expect(fake.current).ToNot(BeNil())
		Expect(fake.current.ID).To(Equal("1"))
	})

	It("starts playback on skip after the previous queue ended (DSub album change)", func() {
		// Play an album and let it finish: the backend goes idle.
		_, _ = pd.Set(ctx, []string{"1", "2"})
		_, _ = pd.Start(ctx)
		fake.idle = true
		fake.playing = false

		// DSub loads a new album with set + skip (no explicit start).
		_, _ = pd.Set(ctx, []string{"3"})
		fake.current = nil
		_, _ = pd.Skip(ctx, 0, 0)

		Expect(fake.current).ToNot(BeNil())
		Expect(fake.current.ID).To(Equal("3"))
		Expect(fake.playing).To(BeTrue()) // playing, not left paused
	})

	It("advances via the trackSwitcher goroutine when the backend reports an advance", func() {
		_, _ = pd.Set(ctx, []string{"1", "2", "3"})
		_, _ = pd.Start(ctx)
		Expect(fake.onAdvance).ToNot(BeNil())

		// Fire the callback the mpv backend would call on a gapless advance; it
		// hands off to the trackSwitcher goroutine. Reading state back through
		// the locked Status() keeps this race-free.
		fake.onAdvance()
		Eventually(func() int {
			st, _ := pd.Status(ctx)
			return st.CurrentIndex
		}).Should(Equal(1))
	})
})
