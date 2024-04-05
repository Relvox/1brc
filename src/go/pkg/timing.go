package pkg

import (
	"fmt"
	"log"
	"sort"
	"sync/atomic"
	"time"
)

// AtomicDuration allows for atomic updates to a time.Duration value.
type AtomicDuration int64

func (a *AtomicDuration) Add(d time.Duration) {
	atomic.AddInt64((*int64)(a), int64(d))
}

func (a *AtomicDuration) Since(start time.Time) {
	stop := time.Now()
	a.Add(stop.Sub(start))
}

func (a *AtomicDuration) Duration() time.Duration {
	return time.Duration(atomic.LoadInt64((*int64)(a)))
}

type TEvent struct {
	Time time.Time
	Text string
}

func (e TEvent) String() string {
	return fmt.Sprint(e.Time.UnixNano(), " ", e.Text)
}

type Timings struct {
	Start       time.Time
	Since_Setup time.Duration

	// ReadFile         time.Time
	Since_ReadFile   time.Duration
	SendBlocks       time.Time
	Since_SendBlocks time.Duration

	ParseBlocks       time.Time
	Since_ParseBlock  time.Duration
	SendBatches       time.Time
	Since_SendBatches AtomicDuration
	Since_WaitParse   time.Duration

	MapData       time.Time
	Since_MapData time.Duration
	SendOutput    AtomicDuration
	Since_WaitMap time.Duration

	Merge           time.Time
	Since_Merge     time.Duration
	Since_MergeWait time.Duration
	Since_Sort      time.Duration
	Since_Build     time.Duration
	Since_Print     time.Duration

	Events    []TEvent
	ChanEvent chan TEvent
}

func (t *Timings) SendEvent(tNow time.Time, text string) {
	t.ChanEvent <- TEvent{Time: tNow, Text: text}
}

const (
	CHANS = 11
)

func (t Timings) Report() {
	close(t.ChanEvent)
	for e := range t.ChanEvent {
		t.Events = append(t.Events, e)
	}
	t.Events = append(t.Events, TEvent{time.Now(), "Done"})
	sort.Slice(t.Events, func(i, j int) bool {
		return t.Events[i].Time.Compare(t.Events[j].Time) < 0
	})

	start := t.Events[0].Time
	for _, v := range t.Events {
		log.Println(v.Time.Sub(start), v.Text)
	}

	Plot(t.Events)

	log.Printf(`
? Setup: %v
[ Read: %v
  > Send: %v
[ Parse: %v
  > Send: %v
  > Wait: %v
[ MapData: %v
  > Send: %v
  > Wait: %v
! Merge: %v
  > Wait: %v
! Sort: %v
! Build: %v
! Print: %v
= Total: %v
		 `,
		t.Since_Setup,

		t.Since_ReadFile-t.Since_SendBlocks,
		t.Since_SendBlocks,

		t.Since_ParseBlock,
		t.Since_SendBatches.Duration()/CHANS,
		t.Since_WaitParse,

		t.Since_MapData,
		t.SendOutput.Duration(),
		t.Since_WaitMap,

		t.Since_Merge,
		t.Since_MergeWait-t.Since_Merge,

		t.Since_Sort,
		t.Since_Build,
		t.Since_Print,
		time.Since(t.Start),
	)
}
