//go:build integration

package store

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

// twin is a second router of this shard: the same databases, the same shard name.
func (s *ShardedStoreSuite) twin() *Store {
	twin, err := OpenShards(Options{DSN: s.dsn, ShardDSN: s.shardDSN, Shards: map[string]string{"here": s.shardDSN, "there": s.other.shardDSN}})
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.NoError(twin.Close()) })
	s.Require().Equal("here", twin.ShardName())
	return twin
}

// working runs a worker of the other shard until the test ends.
func (s *ShardedStoreSuite) working() {
	ctx, cancel := context.WithCancel(s.ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.other.ShardJobWorker(slog.Default(), 20*time.Millisecond).Run(ctx)
	}()
	s.T().Cleanup(func() {
		cancel()
		<-done
	})
}

// runOf is a shard's run of the only job queued.
func (s *ShardedStoreSuite) runOf(shard string) ShardJobRun {
	var run ShardJobRun
	s.Require().NoError(s.store.global.NewSelect().Model(&run).Where("shard = ?", shard).Scan(s.ctx))
	return run
}

// leftBy writes, on a shard, what the user ada leaves there: a request billed to organization,
// a call leg, an agent log and a session with a hidden branch message.
func (s *ShardedStoreSuite) leftBy(shard *Store, organization string) {
	s.Require().NoError(insertRequest(s.ctx, shard, &Request{Modality: "llm", CustomerID: "app", OrganizationID: organization,
		UserID: "ada", Provider: "openai", Model: "gpt", StartedAt: s.base}))
	_, err := shard.ShardDB().NewInsert().Model(&CallLeg{CustomerID: "app", UserID: "ada", CallType: "phone", CallID: "call-" + uuid.NewString(),
		Vendor: "twilio", Direction: Outbound, LocalNumber: "+15550100", RemoteNumber: "+15550199", DialedAt: s.base}).Exec(s.ctx)
	s.Require().NoError(err)
	s.Require().NoError(shard.AgentLogs.Insert(s.ctx, &AgentLog{CustomerID: "app", UserID: "ada", Source: "agent", Severity: "info",
		EventType: "turn", Message: "a turn"}))
	session := "session-" + uuid.NewString()
	s.Require().NoError(shard.Sessions.Upsert(s.ctx, &AgentSession{ID: session, CustomerID: "app", AgentName: "docs", UserID: "ada",
		CallerKind: "user", CreatedAt: s.base}))
	s.Require().NoError(shard.BranchMessages.UpsertMany(s.ctx, []AgentBranchMessage{{SessionID: session, OriginalMessageID: "m-" + session,
		CustomerID: "app", ResponseID: "second", SwitchID: "switch", ChatCreatedAt: s.base, AuthorID: "ada", Message: map[string]any{"text": "hi"}}}))
}

// requestsOf is how many requests of the app on a shard name user.
func (s *ShardedStoreSuite) requestsOf(shard *Store, user string) int64 {
	count, err := shard.Requests.CountByUser(s.ctx, "app", user)
	s.Require().NoError(err)
	return count
}

func (s *ShardedStoreSuite) TestForgettingAUserWaitsForEveryShardAndSumsWhatEachDid() {
	s.leftBy(s.store, "org-a")
	s.leftBy(s.store, "org-a")
	s.leftBy(s.other, "org-b")
	type answer struct {
		forgotten UserShards
		err       error
	}
	answered := make(chan answer, 1)
	go func() {
		ctx, cancel := context.WithTimeout(s.ctx, time.Minute)
		defer cancel()
		forgotten, err := s.store.ForgetUserOnShards(ctx, "app", "ada", "forgotten:x", false)
		answered <- answer{forgotten, err}
	}()

	s.Eventually(func() bool {
		var done int
		s.Require().NoError(s.store.global.NewRaw("SELECT count(*) FROM shard_job_runs WHERE shard = 'here' AND state = 'done'").Scan(s.ctx, &done))
		return done == 1
	}, 10*time.Second, 10*time.Millisecond, "this shard ran its part")
	s.Never(func() bool { return len(answered) > 0 }, time.Second, 10*time.Millisecond, "it answered before the other shard ran its part")
	s.EqualValues(1, s.requestsOf(s.other, "ada"), "the other shard's rows are its own to rewrite")

	ran, err := s.other.ShardJobs.RunDue(s.ctx)
	s.Require().NoError(err)
	s.Equal(1, ran)

	var got answer
	select {
	case got = <-answered:
	case <-time.After(10 * time.Second):
		s.FailNow("it did not answer once every shard ran its part")
	}
	s.Require().NoError(got.err)
	s.Equal(UserShards{Requests: 3, CallLegs: 3, BranchMessages: 3, AgentLogs: 3, Organizations: []string{"org-a", "org-b"}}, got.forgotten)
	s.Zero(s.requestsOf(s.store, "ada"))
	s.Zero(s.requestsOf(s.other, "ada"))
	s.EqualValues(1, s.requestsOf(s.other, "forgotten:x"))
	jobs, err := s.store.global.NewSelect().Model((*ShardJob)(nil)).Count(s.ctx)
	s.Require().NoError(err)
	s.Zero(jobs, "the job, which names the user, is gone once read")
}

func (s *ShardedStoreSuite) TestAWaitThatEndsFirstNamesTheShardBehindWhichStillRunsItsPart() {
	s.leftBy(s.store, "org-a")
	s.leftBy(s.other, "org-b")
	ctx, cancel := context.WithTimeout(s.ctx, time.Second)
	defer cancel()

	_, err := s.store.ForgetUserOnShards(ctx, "app", "ada", "forgotten:x", false)

	s.Require().ErrorIs(err, ErrShardJobPending)
	s.Contains(err.Error(), "shards there")
	s.Zero(s.requestsOf(s.store, "ada"))
	s.EqualValues(1, s.requestsOf(s.other, "ada"))
	ran, err := s.other.ShardJobs.RunDue(s.ctx)
	s.Require().NoError(err)
	s.Equal(1, ran)
	s.Zero(s.requestsOf(s.other, "ada"), "the shard behind ran it later")
}

func (s *ShardedStoreSuite) TestAShardsRunIsRunOnceWhateverHowManyOfItsRoutersLook() {
	twin := s.twin()
	_, err := s.store.ShardJobs.queue(s.ctx, s.store.global, ShardJobClaimGuest, "app", claimGuestArgs{GuestID: "guest-1", UserID: "jlahey", At: s.base})
	s.Require().NoError(err)
	claimed, err := claim[ShardJobRun](s.ctx, s.store.global, shardJobRuns, shardJobLease, shardJobBatch, func(query *bun.SelectQuery) *bun.SelectQuery {
		return query.Where("sjr.shard = ?", "here")
	}, shardJobRunClaimed)
	s.Require().NoError(err)
	s.Require().Len(claimed, 1, "a router of this shard is running it")

	ran, err := twin.ShardJobs.RunDue(s.ctx)
	s.Require().NoError(err)
	s.Zero(ran, "the other router of the shard took a run one of its routers holds")

	s.Require().NoError(s.store.ShardJobs.release(s.ctx, claimed, 0))
	var wg sync.WaitGroup
	counts := make([]int, 2)
	for i, router := range []*Store{s.store, twin} {
		wg.Go(func() {
			var err error
			counts[i], err = router.ShardJobs.RunDue(s.ctx)
			s.NoError(err)
		})
	}
	wg.Wait()
	s.Equal(1, counts[0]+counts[1], "the two routers of the shard ran it once between them")
	_, err = s.store.global.NewUpdate().Model((*ShardJobRun)(nil)).Set("next_attempt_at = now() - interval '1 second'").Where("true").Exec(s.ctx)
	s.Require().NoError(err)
	done := s.runOf("here")
	again, err := twin.ShardJobs.RunDue(s.ctx)
	s.Require().NoError(err)
	s.Zero(again)
	s.Equal(done.Attempts, s.runOf("here").Attempts, "a run done is not claimed again")
	s.Equal(shardJobDone, s.runOf("here").State)
	s.Equal(shardJobQueued, s.runOf("there").State, "each shard runs its own")
}

func (s *ShardedStoreSuite) TestARunOfAKindThisRouterDoesNotKnowIsLeftForOneThatDoes() {
	_, err := s.store.global.NewRaw(`WITH job AS (INSERT INTO shard_jobs (id, kind, args) VALUES ('later', 'from_a_newer_release', '{}'))
		INSERT INTO shard_job_runs (job_id, shard) VALUES ('later', 'here')`).Exec(s.ctx)
	s.Require().NoError(err)

	ran, err := s.store.ShardJobs.RunDue(s.ctx)

	s.Require().NoError(err)
	s.Zero(ran)
	run := s.runOf("here")
	s.Equal(shardJobQueued, run.State)
	s.Zero(run.Attempts, "never claimed")
}

func (s *ShardedStoreSuite) TestAGuestsSessionsInEveryShardAreClaimedOnceEachShardRuns() {
	s.Require().NoError(s.store.Users.InsertGuest(s.ctx, &User{ID: "guest-1", CustomerID: "app", CreatedAt: s.base}))
	s.opened("here", "app", s.base, func(session *AgentSession) { session.UserID = "guest-1"; session.CallerKind = "guest" })
	there := &AgentSession{ID: "there", CustomerID: "app", AgentName: "docs", UserID: "guest-1", CallerKind: "guest", CreatedAt: s.base}
	s.Require().NoError(s.other.Sessions.Upsert(s.ctx, there))

	moved, err := s.store.Users.ClaimGuest(s.ctx, "app", "guest-1", "jlahey")

	s.Require().NoError(err)
	s.EqualValues(1, moved, "this shard's session moved at once")
	here, err := s.store.Sessions.Get(s.ctx, "app", "here")
	s.Require().NoError(err)
	s.Equal([]string{"jlahey", UserKindAuthenticated}, []string{here.UserID, here.CallerKind})
	still, err := s.other.Sessions.Get(s.ctx, "app", "there")
	s.Require().NoError(err)
	s.Equal("guest-1", still.UserID, "the other shard's session is its own to move")

	ran, err := s.other.ShardJobs.RunDue(s.ctx)
	s.Require().NoError(err)
	s.Equal(1, ran)
	moved2, err := s.other.Sessions.Get(s.ctx, "app", "there")
	s.Require().NoError(err)
	s.Equal([]string{"jlahey", UserKindAuthenticated}, []string{moved2.UserID, moved2.CallerKind})

	again, err := s.store.Users.ClaimGuest(s.ctx, "app", "guest-1", "jlahey")
	s.Require().NoError(err, "the same user claiming again finishes a claim rather than failing")
	s.Zero(again)
	_, err = s.store.Users.ClaimGuest(s.ctx, "app", "guest-1", "randy")
	s.ErrorIs(err, ErrGuestClaimed)
}

func (s *ShardedStoreSuite) TestLegacyPinsAreEveryShardsCountSummed() {
	s.working()
	s.opened("here", "acme", s.base, nil)
	for _, id := range []string{"there-1", "there-2"} {
		s.Require().NoError(s.other.Sessions.Upsert(s.ctx, &AgentSession{ID: id, CustomerID: "acme", AgentName: "docs", UserID: "jlahey",
			CallerKind: "user", CreatedAt: s.base}))
	}
	s.Require().NoError(s.other.Sessions.Upsert(s.ctx, &AgentSession{ID: "pinned", CustomerID: "acme", AgentName: "docs", UserID: "jlahey",
		CallerKind: "user", CreatedAt: s.base, StreamAppPK: 99}))
	s.Require().NoError(s.other.Calls.Start(s.ctx, &Call{ID: "call", CustomerID: "acme", StartedAt: s.base}))
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()

	counted, err := s.store.LegacyStreamPins(ctx, 7)

	s.Require().NoError(err)
	s.ElementsMatch([]LegacyCount{{Kind: "session", CustomerID: "acme", Rows: 3}, {Kind: "call", CustomerID: "acme", Rows: 1}}, counted)
}
